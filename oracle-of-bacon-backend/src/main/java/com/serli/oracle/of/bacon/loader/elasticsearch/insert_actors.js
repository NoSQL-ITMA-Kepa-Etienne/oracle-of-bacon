const elasticsearch = require('elasticsearch');
const csv = require('csv-parser');
const fs = require('fs');

const client = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'info'
});

// Création de l'indice
client.indices.create({ index: 'imdb' }, (err, resp) => {});

let actors = [];
fs
  .createReadStream('C:\\Users\\Lea\\Desktop\\NoSQL\\imdb-data\\actors4.csv')
  .pipe(csv())
  // Pour chaque ligne on créé un document JSON pour l'acteur correspondant
  .on('data', data => {
    actors.push({
      name: data.name
    });
  })
  // A la fin on créé l'ensemble des acteurs dans ElasticSearch
  .on('end', () => {
    client.bulk(createBulkInsertQuery(actors), (err, resp) => {
      if (err) console.trace(err.message);
      else console.log(`Inserted ${resp.items.length} actors`);
      client.close();
    });
  });

// Fonction utilitaire permettant de formatter les données pour l'insertion "bulk" dans elastic
function createBulkInsertQuery(actors) {
  const body = actors.reduce((acc, actor) => {
    const { name } = actor;
    acc.push({ index: { _index: 'imdb', _type: 'actor', _id: actor.imdb_id } })
    acc.push({ name, suggest: {
      type: "completion",
      analyzer: "simple",
      preserve_separators: true,
      preserve_position_increments: true,
      max_input_length: 50
    } })
    return acc
  }, []);

  return { body };
}
