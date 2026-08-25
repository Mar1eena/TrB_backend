#!/bin/bash
set -e

# usage: file_env VAR [DEFAULT]
#    ie: file_env 'XYZ_DB_PASSWORD' 'example'
# (will allow for "$XYZ_DB_PASSWORD_FILE" to fill in the value of
#  "$XYZ_DB_PASSWORD" from a file, especially for Docker's secrets feature)
file_env() {
	local var="$1"
	local fileVar="${var}_FILE"
	local def="${2:-}"
	if [ "${!var:-}" ] && [ "${!fileVar:-}" ]; then
		echo >&2 "error: both $var and $fileVar are set (but are exclusive)"
		exit 1
	fi
	local val="$def"
	if [ "${!var:-}" ]; then
		val="${!var}"
	elif [ "${!fileVar:-}" ]; then
		val="$(< "${!fileVar}")"
	fi
	export "$var"="$val"
	unset "$fileVar"
}

# Подключает ./pg-config/logging.conf (смонтирован в /etc/pg-config)
ensure_pg_config_include() {
	local conf="${PGDATA}/postgresql.conf"
	local include_line="include_if_exists = '/etc/pg-config/logging.conf'"
	if [ -f "$conf" ] && ! grep -Fq "/etc/pg-config/logging.conf" "$conf" 2>/dev/null; then
		{
			echo
			echo "# Project logging config (mounted from ./pg-config)"
			echo "$include_line"
		} >> "$conf"
	fi
}

# После kill контейнера в volume может остаться postmaster.pid с PID,
# который в новом контейнере принадлежит entrypoint (часто PID 1).
cleanup_stale_postmaster_pid() {
	local pidfile="${PGDATA}/postmaster.pid"
	[ -f "$pidfile" ] || return 0

	local pid
	pid="$(head -1 "$pidfile" 2>/dev/null || true)"
	if [ -z "$pid" ]; then
		rm -f "$pidfile"
		return 0
	fi

	if ! kill -0 "$pid" 2>/dev/null; then
		echo "Removing stale postmaster.pid (process $pid not running)"
		rm -f "$pidfile"
		return 0
	fi

	local cmd
	cmd="$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null || true)"
	case "$cmd" in
		*bin/postgres*|*postmaster*) ;;
		*)
			echo "Removing stale postmaster.pid (pid $pid is not postgres: $cmd)"
			rm -f "$pidfile"
			;;
	esac
}

if [ "${1:0:1}" = '-' ]; then
	set -- postgres "$@"
fi

# allow the container to be started with `--user`
if [ "$1" = 'postgres' ] && [ "$(id -u)" = '0' ]; then
	mkdir -p "$PGDATA"
	chown -R postgres "$PGDATA"
	chmod 700 "$PGDATA"

	mkdir -p /var/run/postgresql
	chown -R postgres /var/run/postgresql
	chmod g+s /var/run/postgresql

	mkdir -p /var/log/postgresql
	chown -R postgres /var/log/postgresql
	chmod 755 /var/log/postgresql

	exec gosu postgres "$BASH_SOURCE" "$@"
fi

if [ "$1" = 'postgres' ]; then
	mkdir -p "$PGDATA"
	chown -R "$(id -u)" "$PGDATA" 2>/dev/null || :
	chmod 700 "$PGDATA" 2>/dev/null || :

	mkdir -p /var/log/postgresql
	chown -R "$(id -u)" /var/log/postgresql 2>/dev/null || :
	chmod 755 /var/log/postgresql 2>/dev/null || :

	# look specifically for PG_VERSION, as it is expected in the DB dir
	if [ ! -s "$PGDATA/PG_VERSION" ]; then
		file_env 'POSTGRES_INITDB_ARGS'
		eval "initdb --username=postgres $POSTGRES_INITDB_ARGS"

		# check password first so we can output the warning before postgres
		# messes it up
		file_env 'POSTGRES_PASSWORD'
		if [ "$POSTGRES_PASSWORD" ]; then
			pass="PASSWORD '$POSTGRES_PASSWORD'"
			authMethod=md5
		else
			# The - option suppresses leading tabs but *not* spaces. :)
			cat >&2 <<-'EOWARN'
				****************************************************
				WARNING: No password has been set for the database.
				         This will allow anyone with access to the
				         Postgres port to access your database. In
				         Docker's default configuration, this is
				         effectively any other container on the same
				         system.

				         Use "-e POSTGRES_PASSWORD=password" to set
				         it in "docker run".
				****************************************************
			EOWARN

			pass=
			authMethod=trust
		fi

		{ echo; echo "host all all all $authMethod"; } | tee -a "$PGDATA/pg_hba.conf" > /dev/null

		# internal start of server in order to allow set-up using psql-client		
		# does not listen on external TCP/IP and waits until start finishes
		PGUSER="${PGUSER:-postgres}" \
		pg_ctl -D "$PGDATA" \
			-o "-c listen_addresses='localhost'" \
			-w start

		file_env 'POSTGRES_USER' 'postgres'
		file_env 'POSTGRES_DB' "$POSTGRES_USER"

		psql=( psql -v ON_ERROR_STOP=1 )

		if [ "$POSTGRES_DB" != 'postgres' ]; then
			"${psql[@]}" --username postgres <<-EOSQL
				CREATE DATABASE "$POSTGRES_DB" ;
			EOSQL
			echo
		fi

		if [ "$POSTGRES_USER" = 'postgres' ]; then
			op='ALTER'
		else
			op='CREATE'
		fi
		"${psql[@]}" --username postgres <<-EOSQL
			$op USER "$POSTGRES_USER" WITH SUPERUSER $pass ;
		EOSQL
		echo

		psql+=( --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" )

		echo
		for f in /docker-entrypoint-initdb.d/*; do
			case "$f" in
				*.sh)     echo "$0: running $f"; . "$f" ;;
				*.sql)    echo "$0: running $f"; "${psql[@]}" -f "$f"; echo ;;
				*.sql.gz) echo "$0: running $f"; gunzip -c "$f" | "${psql[@]}"; echo ;;
				*)        echo "$0: ignoring $f" ;;
			esac
			echo
		done

		PGUSER="${PGUSER:-postgres}" \
		pg_ctl -D "$PGDATA" -m fast -w stop

		echo
		echo 'PostgreSQL init process complete; ready for start up.'
		echo
	else
		# Кластер уже существует (например, создан пакетом): донастраиваем доступ и роль.
		# В Postgres Pro 1C БД "postgres" часто отсутствует — работаем через template1.
		file_env 'POSTGRES_PASSWORD'
		file_env 'POSTGRES_USER' 'postgres'
		file_env 'POSTGRES_DB' "$POSTGRES_USER"

		if [ -n "$POSTGRES_PASSWORD" ]; then
			authMethod=md5
			pass="PASSWORD '$POSTGRES_PASSWORD'"
		else
			authMethod=trust
			pass=
		fi

		if ! grep -qE '^host\s+all\s+all\s+0\.0\.0\.0/0' "$PGDATA/pg_hba.conf"; then
			echo "host all all 0.0.0.0/0 $authMethod" >> "$PGDATA/pg_hba.conf"
		fi
		if ! grep -qE '^host\s+all\s+all\s+::/0' "$PGDATA/pg_hba.conf"; then
			echo "host all all ::/0 $authMethod" >> "$PGDATA/pg_hba.conf"
		fi

		# Убираем хвост от прошлого падения / kill контейнера
		cleanup_stale_postmaster_pid

		PGUSER="${PGUSER:-postgres}" \
		pg_ctl -D "$PGDATA" \
			-o "-c listen_addresses='localhost' -c logging_collector=off" \
			-w start

		# template1 есть всегда; БД postgres в 1c-сборках может отсутствовать
		psql=( psql -v ON_ERROR_STOP=1 --username postgres --dbname template1 )

		if [ "$POSTGRES_USER" != 'postgres' ]; then
			"${psql[@]}" <<-EOSQL
				DO \$\$
				BEGIN
				  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '$POSTGRES_USER') THEN
				    CREATE ROLE "$POSTGRES_USER" WITH LOGIN SUPERUSER $pass;
				  ELSE
				    ALTER ROLE "$POSTGRES_USER" WITH LOGIN SUPERUSER $pass;
				  END IF;
				END
				\$\$;
			EOSQL
		elif [ -n "$POSTGRES_PASSWORD" ]; then
			"${psql[@]}" -c "ALTER ROLE postgres WITH PASSWORD '$POSTGRES_PASSWORD';"
		fi

		# Совместимость с инструментами, ожидающими БД postgres
		"${psql[@]}" -tc "SELECT 1 FROM pg_database WHERE datname = 'postgres'" | grep -q 1 \
			|| "${psql[@]}" -c "CREATE DATABASE postgres OWNER postgres;"

		if [ -n "$POSTGRES_DB" ] && [ "$POSTGRES_DB" != 'postgres' ] && [ "$POSTGRES_DB" != 'template1' ]; then
			"${psql[@]}" -tc "SELECT 1 FROM pg_database WHERE datname = '$POSTGRES_DB'" | grep -q 1 \
				|| "${psql[@]}" -c "CREATE DATABASE \"$POSTGRES_DB\" OWNER \"$POSTGRES_USER\";"
		fi

		# ALTER SYSTEM (postgresql.auto.conf) перекрывает ./pg-config — сбрасываем дубли
		# (каждый RESET отдельно: ALTER SYSTEM нельзя в одной транзакции пачкой)
		for param in \
			session_preload_libraries \
			auto_explain.log_min_duration \
			auto_explain.log_analyze \
			auto_explain.log_buffers \
			auto_explain.log_wal \
			auto_explain.log_timing \
			auto_explain.log_nested_statements \
			auto_explain.log_verbose \
			auto_explain.log_triggers \
			auto_explain.log_settings \
			auto_explain.log_format \
			auto_explain.sample_rate \
			log_statement \
			log_min_duration_statement \
			log_duration \
			log_parameter_max_length \
			log_parameter_max_length_on_error \
			shared_preload_libraries
		do
			"${psql[@]}" -c "ALTER SYSTEM RESET ${param};" >/dev/null 2>&1 || true
		done

		PGUSER="${PGUSER:-postgres}" \
		pg_ctl -D "$PGDATA" -m fast -w stop
	fi

	ensure_pg_config_include
fi

exec "$@"