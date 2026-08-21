// One-shot: fix HyperDX connection password to match ClickHouse CLICKHOUSE_PASSWORD.
const { MongoClient } = require('mongodb');

async function main() {
  const uri = process.env.MONGO_URI || 'mongodb://127.0.0.1:27017/hyperdx';
  const password = process.env.CLICKHOUSE_PASSWORD || 'default';
  const username = process.env.CLICKHOUSE_USER || process.env.CLICKHOUSE_USERNAME || 'default';
  const host = process.env.HDX_CH_HOST || 'http://127.0.0.1:8123';

  const client = new MongoClient(uri);
  await client.connect();
  const db = client.db();
  const col = db.collection('connections');
  const before = await col.find({}).toArray();
  console.log('connections before:', JSON.stringify(before, null, 2));

  if (before.length === 0) {
    const r = await col.insertOne({
      name: 'Local ClickHouse',
      host,
      username,
      password,
      team: '_local_team_',
      createdAt: new Date(),
      updatedAt: new Date(),
    });
    console.log('inserted connection', r.insertedId.toString());
  } else {
    const r = await col.updateMany(
      {},
      { $set: { password, username, host, updatedAt: new Date() } }
    );
    console.log('updated', r.modifiedCount, 'connection(s)');
  }

  const after = await col.find({}).project({ name: 1, host: 1, username: 1, password: 1 }).toArray();
  console.log('connections after:', JSON.stringify(after, null, 2));
  await client.close();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
