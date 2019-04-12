using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using Neo;
using Neo.IO.Data.LevelDB;
using Neo.IO.Json;
using Neo.Ledger;
using Neo.Network.P2P.Payloads;
using Neo.Plugins;
using Neo.SmartContract;
using Neo.VM;
using Neo.Wallets;
using Snapshot = Neo.Persistence.Snapshot;

namespace BalanceToMongo
{
    public class BalanceToMongo : Plugin, IPersistencePlugin
    {
        //private readonly DB db;

        // public override string Name => "ApplicationLogs";

        public BalanceToMongo()
        {
            //db = DB.Open(Path.GetFullPath(Settings.Default.Path), new Options {CreateIfMissing = true});
        }


        public override void Configure()
        {
            // Console.WriteLine("Configure");

            Settings.Load(GetConfiguration());

            // throw new NotImplementedException();
        }


        public void OnPersist(Snapshot snapshot, IReadOnlyList<Blockchain.ApplicationExecuted> applicationExecutedList)
        {
            // Console.WriteLine("OnPersist");

            // Console.WriteLine("Settings.Default.Conn {0}", Settings.Default.Conn);

            //Console.WriteLine("OnPersist snapshot.PersistingBlock.Index index {0}", snapshot.PersistingBlock.Index);
            if (snapshot.PersistingBlock.Index == 0) return;


            //Console.WriteLine("11");
            UInt256 hash = Blockchain.Singleton.GetBlockHash(snapshot.PersistingBlock.Index - 1);
            if (hash == null) return;

            var mIndex = GetMongoIndex();

            // Console.WriteLine("OnPersist mIndex {0}", mIndex);
            if (snapshot.PersistingBlock.Index - 1 < mIndex) return;


            var block = Blockchain.Singleton.GetBlock(hash);

            // Console.WriteLine("OnPersist block index {0}", block.Index);

            //  Console.WriteLine("this.settings: {0}", Settings.Default.Conn);

            /* 
             var block = snapshot.PersistingBlock;
             Console.WriteLine("OnPersist block index {0}", block.ToJson());
 
             if (block.Index == 0)
             {
                 SaveMongo("0xc56f33fc6ecfcd0c225c4ab356fee59390af8560be0e930faebe74a6daff7c9b",
                     "AQVh2pG732YvtNaxEGkQUei3YA4cvo7d2i", decimal.Parse("100000000"));
             }
             
             */
            foreach (Transaction tx in block.Transactions)
            {
                // Console.WriteLine("OnPersist Transaction {0}", tx.ToJson());

                foreach (CoinReference vin in tx.Inputs)
                {
                    // Console.WriteLine("OnPersist CoinReference vin {0}", vin.ToJson());
                    // OnPersist CoinReference vin {"txid":"0xc800cae5063ccb762d727e167911e65ab2aa489eaf5068f09153ea34188bee98","vout":0}
                    //  var json = new JObject();
                    // json = vin.ToJson();


//                    Console.WriteLine("OnPersist CoinReference vout {0} vin.PrevHash", vin.PrevHash);
//                    Console.WriteLine("OnPersist CoinReference vout {0} vin.PrevIndex", vin.PrevIndex);


                    var transactionState =
                        snapshot.Transactions.TryGet(vin.PrevHash);


//                    Console.WriteLine("OnPersist CoinReference vin transactionState {0}", transactionState.Transaction.Outputs[vin.PrevIndex]);
//                    Console.WriteLine("OnPersist CoinReference vin transactionState {0}", transactionState.Transaction.Outputs[vin.PrevIndex].AssetId);
//                    
                    var utxo = transactionState.Transaction.Outputs[vin.PrevIndex];


                    //     //json["utxo"] = utxo.ToJson(vin.PrevIndex);
                    //   Console.WriteLine("OnPersist CoinReference vin utxo {0}", utxo.ToJson());


                    AccountState account = Blockchain.Singleton.Store.GetAccounts().TryGet(utxo.ScriptHash) ??
                                           new AccountState(utxo.ScriptHash);


//                    Console.WriteLine("OnPersist   vin {0}", account.ToJson());
                    foreach (var b in account.Balances)
                    {
                        if (b.Key == utxo.AssetId)
                        {
                            SaveMongo(b.Key.ToString(), utxo.ScriptHash.ToAddress(),
                                decimal.Parse(b.Value.ToString()));
                        }
                    }

                    // Console.WriteLine("OnPersist vin  account {0}", account.ToJson());


                    // OnPersist CoinReference vin {"txid":"0x6137e84e65b302eea351e76dc602e17c0417ba7920cc8a8303987f72a7004654","vout":0,"utxo":{"n":0,"asset":"0xc56f33fc6ecfcd0c225c4ab356fee59390af8560be0e930faebe74a6daff7c9b","value":"19","address":"ALfnhLg7rUyL6Jr98bzzoxz5J7m64fbR4s"}}
                }


                // Console.WriteLine("2");


                foreach (var vout in tx.Outputs)
                {
                    // var address = vout.ScriptHash.;


                    AccountState account = Blockchain.Singleton.Store.GetAccounts().TryGet(vout.ScriptHash);


                    // Console.WriteLine("4:{0}", account.ToJson());

                    foreach (var b in account.Balances)
                    {
                        if (b.Key == vout.AssetId)
                        {
//                            Console.WriteLine("5:{0}", b.Key);
//                            Console.WriteLine("5:{0}", b.Value);
                            SaveMongo(b.Key.ToString(), vout.ScriptHash.ToAddress(),
                                decimal.Parse(b.Value.ToString()));
                        }
                    }

                    //  Console.WriteLine("OnPersist vout  account {0}", account.ToJson());
                }


                
               //  Console.WriteLine("3");
                foreach (var appExec in applicationExecutedList)
                {
                    // Console.WriteLine("1");

                    JObject json = new JObject();

                    json["executions"] = appExec.ExecutionResults.Select(p =>
                    {
                        JObject execution = new JObject();


                        execution["notifications"] = p.Notifications.Select(q =>
                        {
                            JObject notification = new JObject();

                            try
                            {
                                notification["state"] = q.State.ToParameter().ToJson();


                                var value = notification["state"]["value"];


                                if (value is JArray)
                                {
                                    var info = ((JArray) value);

                                    if (info.Count == 4)
                                    {
                                        if (info[0]["value"].AsString() == "7472616e73666572")
                                        {
                                            if (info[1]["type"].AsString() == "ByteArray" &&
                                                !string.IsNullOrEmpty(info[1]["value"].AsString()))
                                            {
                                                var address = new UInt160(info[1]["value"].AsString().HexToBytes())
                                                    .ToAddress();

                                                var bala = GetBalanceOf(q.ScriptHash.ToString(),
                                                    info[1]["value"].AsString());

                                                // Console.WriteLine("OnPersist from address  {0},{1}", info[1]["value"],
                                                //     bala);
                                                SaveMongo(q.ScriptHash.ToString(), address, bala);
                                            }

                                            if (info[2]["type"].AsString() == "ByteArray" &&
                                                !string.IsNullOrEmpty(info[2]["value"].AsString()))
                                            {
                                                var address =
                                                    new UInt160(info[2]["value"].AsString().HexToBytes())
                                                        .ToAddress();

                                                var bala = GetBalanceOf(q.ScriptHash.ToString(),
                                                    info[2]["value"].AsString());

                                                // Console.WriteLine("OnPersist to address  {0},{1}", info[2]["value"],
                                                 //    bala);
                                                SaveMongo(q.ScriptHash.ToString(), address, bala);
                                            }
                                        }
                                    }


                                    //                            
                                }
                            }
                            catch (InvalidOperationException)
                            {
                            }

                            return notification;
                        }).ToArray();
                        return execution;
                    }).ToArray();
                }

                
                //   


                UpdateMongoIndex(snapshot.PersistingBlock.Index - 1);
            }
        }

        public void OnCommit(Snapshot snapshot)
        {
//            AccountState account = Blockchain.Singleton.Store.GetAccounts()
//                .TryGet("AQVh2pG732YvtNaxEGkQUei3YA4cvo7d2i"); 
//                                   new AccountState(utxo.ScriptHash);


//            var block = snapshot.PersistingBlock;
//            Console.WriteLine("OnPersist block index {0}", block.ToJson());


//            Console.WriteLine("OnCommit");
//
//            Console.WriteLine("OnPersist snapshot.PersistingBlock.Index index {0}", snapshot.PersistingBlock.Index);
//            if(snapshot.PersistingBlock.Index == 0) return;
//
//
//            Console.WriteLine("11");
//            UInt256 hash = Blockchain.Singleton.GetBlockHash(snapshot.PersistingBlock.Index - 1);
//            if (hash == null) return ;
//            var block = Blockchain.Singleton.GetBlock(hash);
//            
//            Console.WriteLine("OnPersist block index {0}", block.ToJson());
        }


        bool IPersistencePlugin.ShouldThrowExceptionFromCommit(Exception ex)
        {
            //  Console.WriteLine("ShouldThrowExceptionFromCommit");
            return false;
            //throw new NotImplementedException();
        }


        public int GetDecimals(string contract)
        {
//            Console.WriteLine("OnPersist script contract {0}", contract);


            UInt160 script_hash = UInt160.Parse(contract);
            string operation = "decimals";

            byte[] script;
            using (ScriptBuilder sb = new ScriptBuilder())
            {
                script = sb.EmitAppCall(script_hash, operation, new ContractParameter[0]).ToArray();
            }

            ApplicationEngine engine = ApplicationEngine.Run(script, extraGAS: default(Fixed8));
            var stack = new JArray(engine.ResultStack.Select(p => p.ToParameter().ToJson()));


            if (stack.Count == 0)
            {
                return 8;
            }
//
//            Console.WriteLine("OnPersist script decimals {0}", stack[0]["value"].ToString());

            if (stack[0]["value"].AsString() == "")
            {
                return 8;
            }

            // return 1;
            return int.Parse(stack[0]["value"].AsString());
        }

        public string GetSymbol(string contract)
        {
            UInt160 script_hash = UInt160.Parse(contract);
            string operation = "symbol";

            byte[] script;
            using (ScriptBuilder sb = new ScriptBuilder())
            {
                script = sb.EmitAppCall(script_hash, operation, new ContractParameter[0]).ToArray();
            }

            ApplicationEngine engine = ApplicationEngine.Run(script, extraGAS: default(Fixed8));
            var stack = new JArray(engine.ResultStack.Select(p => p.ToParameter().ToJson()));

            //Console.WriteLine("OnPersist script symbol {0}", stack[0]["value"].AsString().HexToString());


            if (stack.Count == 0)
            {
                return "";
            }

            // Console.WriteLine("OnPersist script GetSymbol {0}", stack[0]["value"].ToString());

            if (stack[0]["value"].AsString() == "")
            {
                return "";
            }

            return stack[0]["value"].AsString();
        }


        public decimal GetBalanceOf(string contract, string address)
        {
//
//            Console.WriteLine("OnPersist script GetBalanceOf contract 1 {0}", contract);
//
//            Console.WriteLine("OnPersist script GetBalanceOf address 2 {0}", address);


            UInt160 script_hash = UInt160.Parse(contract);
            string operation = "balanceOf";

            byte[] script;
            using (ScriptBuilder sb = new ScriptBuilder())
            {
                //  var paramas = new JArray();
//                var paramas = new ContractParameter[1];
//                paramas[0].Type =ContractParameterType.Hash160;
//                paramas[0].Value = address;


                //  paramas.Add(c);

                var obj = new JObject();
                obj["type"] = "Hash160";
                obj["value"] = new UInt160(address.HexToBytes()).ToString();


                // Console.WriteLine("OnPersist script GetBalanceOf obj 3 {0}", obj.ToString());


                var arr = new JArray();
                arr.Add(obj);

                ContractParameter[] parameters = (arr).Select(p => ContractParameter.FromJson(p)).ToArray();
                script = sb.EmitAppCall(script_hash, operation, parameters).ToArray();
            }

            ApplicationEngine engine = ApplicationEngine.Run(script, extraGAS: default(Fixed8));
            var stack = new JArray(engine.ResultStack.Select(p => p.ToParameter().ToJson()));


            if (stack.Count == 0)
            {
                return 0;
            }

//            Console.WriteLine("OnPersist script GetBalanceOf obj 4 {0}", stack.ToString());
//
//            Console.WriteLine("OnPersist script GetBalanceOf 5 {0}", stack[0]["value"].ToString());

            if (stack[0]["value"].AsString() == "")
            {
                return 0;
            }

            // return 1;
//            return int.Parse(stack[0]["value"].AsString());

            var de = GetDecimals(contract);
            if (stack[0]["type"].AsString() == "ByteArray" &&
                !string.IsNullOrEmpty(stack[0]["value"].AsString()))
            {
                var handleValue =
                    ((new BigInteger(stack[0]["value"].AsString().HexToBytes())) /
                     new BigInteger(Math.Pow(10, de
                     )));

//                                            var handleValue = ((new BigInteger("00ac23fc06".HexToBytes())) /
//                                                               new BigInteger(Math.Pow(10, GetDecimals())));


//                Console.WriteLine("OnPersist script GetBalanceOf 5 {0}",
//                    decimal.Parse(handleValue.ToString(), NumberStyles.Any));
                return decimal.Parse(handleValue.ToString(), NumberStyles.Any);
            }


            if (stack[0]["type"].AsString() == "Integer" && !string.IsNullOrEmpty(stack[0]["value"].AsString()))
            {
                var handleValue = (Double.Parse(stack[0]["value"].AsString()) /
                                   Math.Pow(10, de));


                return decimal.Parse(handleValue.ToString(), NumberStyles.Any);
            }

            return 0;
        }


        public uint GetMongoIndex()
        {
            // var ConnectionString = "mongodb://localhost:27017";


            var client = new MongoClient(Settings.Default.Conn);

            var db = client.GetDatabase(Settings.Default.DataBase);


            var col = db.GetCollection<BsonDocument>(Settings.Default.Coll_Status);


            var balanceIndex = col.FindSync(new BsonDocument()).ToList();


            if (balanceIndex.Count == 0)
            {
                col.InsertOne(new BsonDocument()
                {
                    {"index", 0}
                });
                return 0;
            }

            return UInt32.Parse(balanceIndex[0]["index"].ToString());


            // Console.WriteLine("balanceIndex: {0}",balanceIndex[0]["index"]);
        }

        public void UpdateMongoIndex(uint index)
        {
            // var ConnectionString = "mongodb://localhost:27017";


            var client = new MongoClient(Settings.Default.Conn);

            var db = client.GetDatabase(Settings.Default.DataBase);


            var col = db.GetCollection<BsonDocument>(Settings.Default.Coll_Status);


            col.ReplaceOne(new BsonDocument(), new BsonDocument()
            {
                {"index", index}
            });


            // Console.WriteLine("balanceIndex: {0}",balanceIndex[0]["index"]);
        }


        public void SaveMongo(string key, string address, decimal score)
        {
            //  Console.WriteLine("1");
            Console.WriteLine("SaveMongokey: {0},number: {1},score :{2}", key, address, score);
//
//
//           
////            
//            Console.WriteLine("Settings.Default.Conn {0}", Settings.Default.Conn);
//            Console.WriteLine("Settings.Default.DataBase {0}", Settings.Default.DataBase);
//            Console.WriteLine("Settings.Default.Coll_Balance {0}", Settings.Default.Coll_Balance);


            //Console.WriteLine("SaveMongokey: 0 {0}", Settings.Default.Conn);

            // var ConnectionString = "mongodb://localhost:27017";


            var client = new MongoClient(Settings.Default.Conn);
            //Console.WriteLine("SaveMongokey: a");
            var db = client.GetDatabase(Settings.Default.DataBase);
            //Console.WriteLine("SaveMongokey: b");

            var col = db.GetCollection<BsonDocument>(Settings.Default.Coll_Balance);

            //Console.WriteLine("SaveMongokey: 1");
            var doc = new BsonDocument()
            {
                {"assetId", key}, {"address", address}, {"balance", BsonDecimal128.Create(score)}
            };


            var filter = new BsonDocument()
            {
                {"assetId", key}, {"address", address}
            };

            //Console.WriteLine("SaveMongokey: 2");

            var addressAssetBalance = col.FindSync(filter).ToList();

            // Console.WriteLine("SaveMongokey: 443333,{0}", addressAssetBalance.Count);

            if (addressAssetBalance.Count > 0)
            {
                col.ReplaceOne(filter, doc);
            }
            else
            {
                col.InsertOne(doc);
            }


//            Console.WriteLine("SaveMongokey: 444");

            // Console.WriteLine("key: {0},number: {1},score :{2}", key, number, score);

//            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("127.0.0.1:6379");
            //
            //            IDatabase db = redis.GetDatabase();
            //
            //            db.SortedSetAdd(key, number, score);
        }
    }
}