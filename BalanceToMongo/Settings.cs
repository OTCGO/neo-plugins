using System;
using Microsoft.Extensions.Configuration;
using MongoDB.Bson;
using MongoDB.Driver;
using Neo.Network.P2P;

namespace BalanceToMongo
{
    internal class Settings
    {
        public string Conn { get; }
        public string DataBase { get; }
        public string Coll_Balance { get; }
        public string Coll_Status { get; }

        public static Settings Default { get; private set; }

        private Settings(IConfigurationSection section)
        {
            //  this.Path = string.Format(section.GetSection("Path").Value, Message.Magic.ToString("X8"));
            Conn = string.Format(section.GetSection("Conn").Value);
            DataBase = string.Format(section.GetSection("DataBase").Value);
            Coll_Balance = string.Format(section.GetSection("Coll_Balance").Value);
            Coll_Status = string.Format(section.GetSection("Coll_Status").Value);


            SetMongoIndex();
        }

        public static void Load(IConfigurationSection section)
        {
            Default = new Settings(section);
        }

        private void SetMongoIndex()
        {
            var client = new MongoClient(Conn);
            var db = client.GetDatabase(DataBase);

            var col = db.GetCollection<BsonDocument>(Coll_Balance);

            col.Indexes.CreateOne(
                new CreateIndexModel<BsonDocument>(Builders<BsonDocument>.IndexKeys.Descending("balance")));


            col.Indexes.CreateOne(new CreateIndexModel<BsonDocument>(Builders<BsonDocument>.IndexKeys.Combine(
                Builders<BsonDocument>.IndexKeys.Ascending("assetId"),
                Builders<BsonDocument>.IndexKeys.Ascending("address")
            ), new CreateIndexOptions {Unique = true}));
        }
    }
}