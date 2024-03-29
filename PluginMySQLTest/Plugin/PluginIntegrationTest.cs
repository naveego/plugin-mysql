using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Grpc.Core;
using Naveego.Sdk.Plugins;
using Newtonsoft.Json;
using PluginMySQL.DataContracts;
using PluginMySQL.Helper;
using Xunit;
using Record = Naveego.Sdk.Plugins.Record;

namespace PluginMySQLTest.Plugin
{
    public class PluginIntegrationTest
    {
        private const string SETTINGS_HOSTNAME = "";
        private const string SETTINGS_DATABASE = "";
        private const string SETTINGS_PASSWORD = "";
        private const string TARGET_IP = "";

        private Settings GetSettings()
        {
            return new Settings
            {
                Hostname = SETTINGS_HOSTNAME,
                Database = SETTINGS_DATABASE,
                Port = "3306",
                Username = "root",
                Password = SETTINGS_PASSWORD
            };
        }

        private ConnectRequest GetConnectSettings()
        {
            var settings = GetSettings();

            return new ConnectRequest
            {
                SettingsJson = JsonConvert.SerializeObject(settings),
                OauthConfiguration = new OAuthConfiguration(),
                OauthStateJson = ""
            };
        }

        private Schema GetTestSchema(string id = "test", string name = "test", string query = "")
        {
            return new Schema
            {
                Id = id,
                Name = name,
                Query = query,
                Properties =
                {
                    new Property
                    {
                        Id = "Id",
                        Name = "Id",
                        Type = PropertyType.Integer,
                        IsKey = true
                    },
                    new Property
                    {
                        Id = "Name",
                        Name = "Name",
                        Type = PropertyType.String
                    }
                }
            };
        }
        
        private Schema GetTestReplicationSchema(string id = "test", string name = "test", string query = "")
        {
            return new Schema
            {
                Id = id,
                Name = name,
                Query = query,
                Properties =
                {
                    new Property
                    {
                        Id = "Id",
                        Name = "Id",
                        Type = PropertyType.Integer,
                        IsKey = true
                    },
                    new Property
                    {
                        Id = "Name",
                        Name = "Name",
                        Type = PropertyType.String
                    },
                    new Property
                    {
                        Id = "DateTime",
                        Name = "DateTime",
                        Type = PropertyType.Datetime
                    },
                    new Property
                    {
                        Id = "Date",
                        Name = "Date",
                        Type = PropertyType.Date
                    },
                    new Property
                    {
                        Id = "Time",
                        Name = "Time",
                        Type = PropertyType.Time
                    },
                    new Property
                    {
                        Id = "Decimal",
                        Name = "Decimal",
                        Type = PropertyType.Decimal
                    },
                }
            };
        }

        private Schema GetTestReplicationSchema2(string id = "test", string name = "test", string query = "")
        {
            return new Schema
            {
                Id = id,
                Name = name,
                Query = query,
                Properties =
                {
                    new Property
                    {
                        Id = "Id",
                        Name = "Id",
                        Type = PropertyType.Integer,
                        IsKey = true
                    },
                    new Property
                    {
                        Id = "Name",
                        Name = "Name",
                        Type = PropertyType.String
                    },
                    new Property
                    {
                        Id = "Email",
                        Name = "Email",
                        Type = PropertyType.String
                    },
                    new Property
                    {
                        Id = "Date",
                        Name = "Date",
                        Type = PropertyType.Date
                    },
                    new Property
                    {
                        Id = "Time",
                        Name = "Time",
                        Type = PropertyType.Time
                    },
                    new Property
                    {
                        Id = "Decimal",
                        Name = "Decimal",
                        Type = PropertyType.Decimal
                    },
                }
            };
        }
        
        [Fact]
        public async Task ConnectSessionTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginMySQL.Plugin.Plugin())},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var request = GetConnectSettings();
            var disconnectRequest = new DisconnectRequest();

            // act
            var response = client.ConnectSession(request);
            var responseStream = response.ResponseStream;
            var records = new List<ConnectResponse>();

            while (await responseStream.MoveNext())
            {
                records.Add(responseStream.Current);
                client.Disconnect(disconnectRequest);
            }

            // assert
            Assert.Single(records);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task ConnectTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginMySQL.Plugin.Plugin())},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var request = GetConnectSettings();

            // act
            var response = client.Connect(request);

            // assert
            Assert.IsType<ConnectResponse>(response);
            Assert.Equal("", response.SettingsError);
            Assert.Equal("", response.ConnectionError);
            Assert.Equal("", response.OauthError);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }
        
        [Fact]
        public async Task ConnectFailedTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginMySQL.Plugin.Plugin())},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            const string wrongUsername = "rootx";

            var request = new ConnectRequest
            {
                SettingsJson = JsonConvert.SerializeObject(new Settings
                {
                    Hostname = SETTINGS_HOSTNAME,
                    Database = SETTINGS_DATABASE,
                    Port = "3306",
                    Username = wrongUsername,
                    Password = SETTINGS_PASSWORD
                }),
                OauthConfiguration = new OAuthConfiguration(),
                OauthStateJson = ""
            };

            // act
            var response = client.Connect(request);

            // assert
            Assert.IsType<ConnectResponse>(response);
            Assert.Equal("", response.SettingsError);
            Assert.Equal($"Access denied for user '{wrongUsername}'@'{TARGET_IP}' (using password: YES)", response.ConnectionError);
            Assert.Equal("", response.OauthError);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task DiscoverSchemasAllTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginMySQL.Plugin.Plugin())},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var request = new DiscoverSchemasRequest
            {
                Mode = DiscoverSchemasRequest.Types.Mode.All,
                SampleSize = 10
            };

            // act
            client.Connect(connectRequest);
            var response = client.DiscoverSchemas(request);

            // assert
            Assert.IsType<DiscoverSchemasResponse>(response);
            Assert.Equal(17, response.Schemas.Count);

            var schema = response.Schemas[0];
            Assert.Equal("`classicmodels`.`customers`", schema.Id);
            Assert.Equal("classicmodels.customers", schema.Name);
            Assert.Equal("", schema.Query);
            Assert.Equal(10, schema.Sample.Count);
            Assert.Equal(13, schema.Properties.Count);

            var property = schema.Properties[7];
            Assert.Equal("`customerNumber`", property.Id);
            Assert.Equal("customerNumber", property.Name);
            Assert.Equal("", property.Description);
            Assert.Equal(PropertyType.Integer, property.Type);
            Assert.True(property.IsKey);
            Assert.False(property.IsNullable);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task DiscoverSchemasRefreshTableTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginMySQL.Plugin.Plugin())},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var request = new DiscoverSchemasRequest
            {
                Mode = DiscoverSchemasRequest.Types.Mode.Refresh,
                SampleSize = 10,
                ToRefresh = {GetTestSchema("`classicmodels`.`customers`", "classicmodels.customers")}
            };

            // act
            client.Connect(connectRequest);
            var response = client.DiscoverSchemas(request);

            // assert
            Assert.IsType<DiscoverSchemasResponse>(response);
            Assert.Single(response.Schemas);

            var schema = response.Schemas[0];
            Assert.Equal("`classicmodels`.`customers`", schema.Id);
            Assert.Equal("classicmodels.customers", schema.Name);
            Assert.Equal("", schema.Query);
            Assert.Equal(10, schema.Sample.Count);
            Assert.Equal(13, schema.Properties.Count);

            var property = schema.Properties[0];
            Assert.Equal("`customerNumber`", property.Id);
            Assert.Equal("customerNumber", property.Name);
            Assert.Equal("", property.Description);
            Assert.Equal(PropertyType.Integer, property.Type);
            Assert.True(property.IsKey);
            Assert.False(property.IsNullable);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task DiscoverSchemasRefreshQueryTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginMySQL.Plugin.Plugin())},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var request = new DiscoverSchemasRequest
            {
                Mode = DiscoverSchemasRequest.Types.Mode.Refresh,
                SampleSize = 10,
                ToRefresh = {GetTestSchema("test", "test", $"SELECT * FROM `classicmodels`.`customers`")}
            };

            // act
            client.Connect(connectRequest);
            var response = client.DiscoverSchemas(request);

            // assert
            Assert.IsType<DiscoverSchemasResponse>(response);
            Assert.Single(response.Schemas);

            var schema = response.Schemas[0];
            Assert.Equal("test", schema.Id);
            Assert.Equal("test", schema.Name);
            Assert.Equal("SELECT * FROM `classicmodels`.`customers`", schema.Query);
            Assert.Equal(10, schema.Sample.Count);
            Assert.Equal(13, schema.Properties.Count);

            var property = schema.Properties[0];
            Assert.Equal("`customerNumber`", property.Id);
            Assert.Equal("customerNumber", property.Name);
            Assert.Equal("", property.Description);
            Assert.Equal(PropertyType.Integer, property.Type);
            Assert.True(property.IsKey);
            Assert.False(property.IsNullable);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task DiscoverSchemasRefreshQueryBadSyntaxTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginMySQL.Plugin.Plugin())},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var request = new DiscoverSchemasRequest
            {
                Mode = DiscoverSchemasRequest.Types.Mode.Refresh,
                SampleSize = 10,
                ToRefresh = {GetTestSchema("test", "test", $"bad syntax")}
            };

            // act
            client.Connect(connectRequest);

            try
            {
                client.DiscoverSchemas(request);
            }
            catch (Exception e)
            {
                // assert
                Assert.IsType<RpcException>(e);
                Assert.Contains("You have an error in your SQL syntax", e.Message);
            }

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }


        [Fact]
        public async Task ReadStreamTableSchemaTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginMySQL.Plugin.Plugin())},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var schema = GetTestSchema("`classicmodels`.`customers`", "classicmodels.customers");

            var connectRequest = GetConnectSettings();

            var schemaRequest = new DiscoverSchemasRequest
            {
                Mode = DiscoverSchemasRequest.Types.Mode.Refresh,
                ToRefresh = {schema}
            };

            var request = new ReadRequest()
            {
                DataVersions = new DataVersions
                {
                    JobId = "test"
                },
                JobId = "test",
            };

            // act
            client.Connect(connectRequest);
            var schemasResponse = client.DiscoverSchemas(schemaRequest);
            request.Schema = schemasResponse.Schemas[0];

            var response = client.ReadStream(request);
            var responseStream = response.ResponseStream;
            var records = new List<Record>();

            while (await responseStream.MoveNext())
            {
                records.Add(responseStream.Current);
            }

            // assert
            Assert.Equal(122, records.Count);

            var record = JsonConvert.DeserializeObject<Dictionary<string, object>>(records[0].DataJson);
            Assert.Equal((long) 103, record["`customerNumber`"]);
            Assert.Equal("Atelier graphique", record["`customerName`"]);
            Assert.Equal("Schmitt", record["`contactLastName`"]);
            Assert.Equal("Carine", record["`contactFirstName`"]);
            Assert.Equal("40.32.2555", record["`phone`"]);
            Assert.Equal("54, rue Royale", record["`addressLine1`"]);
            Assert.Null(record["`addressLine2`"]);
            Assert.Equal("Nantes", record["`city`"]);
            Assert.Null(record["`state`"]);
            Assert.Equal("44000", record["`postalCode`"]);
            Assert.Equal("France", record["`country`"]);
            Assert.Equal((long) 1370, record["`salesRepEmployeeNumber`"]);
            Assert.Equal("21000.00", record["`creditLimit`"]);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task ReadStreamQuerySchemaTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginMySQL.Plugin.Plugin())},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var schema = GetTestSchema("test", "test", $"SELECT * FROM `classicmodels`.`orders`");

            var connectRequest = GetConnectSettings();

            var schemaRequest = new DiscoverSchemasRequest
            {
                Mode = DiscoverSchemasRequest.Types.Mode.Refresh,
                ToRefresh = {schema}
            };

            var request = new ReadRequest
            {
                DataVersions = new DataVersions
                {
                    JobId = "test"
                },
                JobId = "test",
            };

            // act
            client.Connect(connectRequest);
            var schemasResponse = client.DiscoverSchemas(schemaRequest);
            request.Schema = schemasResponse.Schemas[0];

            var response = client.ReadStream(request);
            var responseStream = response.ResponseStream;
            var records = new List<Record>();

            while (await responseStream.MoveNext())
            {
                records.Add(responseStream.Current);
            }

            // assert
            Assert.Equal(326, records.Count);

            var record = JsonConvert.DeserializeObject<Dictionary<string, object>>(records[0].DataJson);
            Assert.Equal((long) 10100, record["`orderNumber`"]);
            Assert.Equal(DateTime.Parse("2003-01-06"), record["`orderDate`"]);
            Assert.Equal(DateTime.Parse("2003-01-13"), record["`requiredDate`"]);
            Assert.Equal(DateTime.Parse("2003-01-10"), record["`shippedDate`"]);
            Assert.Equal("Shipped", record["`status`"]);
            Assert.Null(record["`comments`"]);
            Assert.Equal((long) 363, record["`customerNumber`"]);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task ReadStreamLimitTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginMySQL.Plugin.Plugin())},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var schema = GetTestSchema("`classicmodels`.`customers`", "classicmodels.customers");

            var connectRequest = GetConnectSettings();

            var schemaRequest = new DiscoverSchemasRequest
            {
                Mode = DiscoverSchemasRequest.Types.Mode.Refresh,
                ToRefresh = {schema}
            };

            var request = new ReadRequest()
            {
                DataVersions = new DataVersions
                {
                    JobId = "test"
                },
                JobId = "test",
                Limit = 10
            };

            // act
            client.Connect(connectRequest);
            var schemasResponse = client.DiscoverSchemas(schemaRequest);
            request.Schema = schemasResponse.Schemas[0];

            var response = client.ReadStream(request);
            var responseStream = response.ResponseStream;
            var records = new List<Record>();

            while (await responseStream.MoveNext())
            {
                records.Add(responseStream.Current);
            }

            // assert
            Assert.Equal(10, records.Count);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task PrepareWriteTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginMySQL.Plugin.Plugin())},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var request = new PrepareWriteRequest()
            {
                Schema = GetTestSchema(),
                CommitSlaSeconds = 1,
                Replication = new ReplicationWriteRequest
                {
                    SettingsJson = JsonConvert.SerializeObject(new ConfigureReplicationFormData
                    {
                        SchemaName = "test",
                        GoldenTableName = "gr_test",
                        VersionTableName = "vr_test"
                    })
                },
                DataVersions = new DataVersions
                {
                    JobId = "jobUnitTest",
                    ShapeId = "shapeUnitTest",
                    JobDataVersion = 1,
                    ShapeDataVersion = 2
                }
            };

            // act
            client.Connect(connectRequest);
            var response = client.PrepareWrite(request);

            // assert
            Assert.IsType<PrepareWriteResponse>(response);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task ReplicationWriteTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginMySQL.Plugin.Plugin())},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var prepareWriteRequest = new PrepareWriteRequest()
            {
                Schema = GetTestReplicationSchema(),
                CommitSlaSeconds = 1000,
                Replication = new ReplicationWriteRequest
                {
                    SettingsJson = JsonConvert.SerializeObject(new ConfigureReplicationFormData
                    {
                        SchemaName = "test",
                        GoldenTableName = "gr_test",
                        VersionTableName = "vr_test"
                    })
                },
                DataVersions = new DataVersions
                {
                    JobId = "jobUnitTest",
                    ShapeId = "shapeUnitTest",
                    JobDataVersion = 1,
                    ShapeDataVersion = 1
                }
            };

            var records = new List<Record>
            {
                new Record
                {
                    Action = Record.Types.Action.Upsert,
                    CorrelationId = "test",
                    RecordId = "record1",
                    DataJson = $"{{\"Id\":1,\"Name\":\"Test Company\",\"DateTime\":\"{DateTime.Today}\",\"Date\":\"{DateTime.Now.Date}\",\"Time\":\"{DateTime.Now:hh:mm:ss}\",\"Decimal\":\"13.04\"}}",
                    Versions =
                    {
                        new RecordVersion
                        {
                            RecordId = "version1",
                            DataJson = $"{{\"Id\":1,\"Name\":\"Test Company\",\"DateTime\":\"{DateTime.Now}\",\"Date\":\"{DateTime.Now.Date}\",\"Time\":\"{DateTime.Now:hh:mm:ss}\",\"Decimal\":\"13.04\"}}",
                        }
                    }
                }
            };

            var recordAcks = new List<RecordAck>();

            // act
            client.Connect(connectRequest);
            client.PrepareWrite(prepareWriteRequest);

            using (var call = client.WriteStream())
            {
                var responseReaderTask = Task.Run(async () =>
                {
                    while (await call.ResponseStream.MoveNext())
                    {
                        var ack = call.ResponseStream.Current;
                        recordAcks.Add(ack);
                    }
                });

                foreach (Record record in records)
                {
                    await call.RequestStream.WriteAsync(record);
                }

                await call.RequestStream.CompleteAsync();
                await responseReaderTask;
            }

            // assert
            Assert.Single(recordAcks);
            Assert.Equal("", recordAcks[0].Error);
            Assert.Equal("test", recordAcks[0].CorrelationId);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

            [Fact]
        public async Task ReplicationEscapeWriteTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginMySQL.Plugin.Plugin())},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var prepareWriteRequest = new PrepareWriteRequest
            {
                Schema = GetTestReplicationSchema2("test2", "test2"),
                CommitSlaSeconds = 1000,
                Replication = new ReplicationWriteRequest
                {
                    SettingsJson = JsonConvert.SerializeObject(new ConfigureReplicationFormData
                    {
                        SchemaName = "test",
                        GoldenTableName = "gr_test_escape",
                        VersionTableName = "vr_test_escape"
                    })
                },
                DataVersions = new DataVersions
                {
                    JobId = "jobUnitTest2",
                    ShapeId = "shapeUnitTest2",
                    JobDataVersion = 1,
                    ShapeDataVersion = 1
                }
            };

            var records = new List<Record>
            {
                new Record
                {
                    Action = Record.Types.Action.Upsert,
                    CorrelationId = "test_escape",
                    RecordId = "record1",
                    DataJson = $"{{\"Id\":1,\"Name\":\"\\\\Test Company\\\\\",\"Email\":\"myjob@testcompany.com\",\"Date\":\"{DateTime.Now.Date}\",\"Time\":\"{DateTime.Now:hh:mm:ss}\",\"Decimal\":\"13.04\"}}",
                    Versions =
                    {
                        new RecordVersion
                        {
                            RecordId = "version1",
                            DataJson = $"{{\"Id\":1,\"Name\":\"\\\\Test Company\\\\\",\"Email\":\"myjob@testcompany.com\",\"Date\":\"{DateTime.Now.Date}\",\"Time\":\"{DateTime.Now:hh:mm:ss}\",\"Decimal\":\"13.04\"}}",
                        }
                    }
                },
                new Record
                {
                    Action = Record.Types.Action.Upsert,
                    CorrelationId = "test_escape",
                    RecordId = "record1",
                    DataJson = $"{{\"Id\":2,\"Name\":\"\\\\\\\\\\\\Test Company\\\\\",\"Email\":\"myjob2@&testcompany.com\",\"Date\":\"{DateTime.Now.Date}\",\"Time\":\"{DateTime.Now:hh:mm:ss}\",\"Decimal\":\"13.04\"}}",
                    Versions =
                    {
                        new RecordVersion
                        {
                            RecordId = "version1",
                            DataJson = $"{{\"Id\":2,\"Name\":\"\\\\\\\\\\\\Test Company\\\\\",\"Email\":\"myjob2@&testcompany.com\",\"Date\":\"{DateTime.Now.Date}\",\"Time\":\"{DateTime.Now:hh:mm:ss}\",\"Decimal\":\"13.04\"}}",
                        }
                    }
                },
                new Record
                {
                    Action = Record.Types.Action.Upsert,
                    CorrelationId = "test_escape",
                    RecordId = "record1",
                    DataJson = $"{{\"Id\":3,\"Name\":\"\\\\\\\\Test \\\\Company\\\\\",\"Email\":\"myjob3\\\\@test\'company.com\",\"Date\":\"{DateTime.Now.Date}\",\"Time\":\"{DateTime.Now:hh:mm:ss}\",\"Decimal\":\"13.04\"}}",
                    Versions =
                    {
                        new RecordVersion
                        {
                            RecordId = "version1",
                            DataJson = $"{{\"Id\":3,\"Name\":\"\\\\\\\\Test \\\\Company\\\\\",\"Email\":\"myjob3\\\\@test\'company.com\",\"Date\":\"{DateTime.Now.Date}\",\"Time\":\"{DateTime.Now:hh:mm:ss}\",\"Decimal\":\"13.04\"}}",
                        }
                    }
                }
            };

            var recordAcks = new List<RecordAck>();

            // act
            client.Connect(connectRequest);
            client.PrepareWrite(prepareWriteRequest);

            using (var call = client.WriteStream())
            {
                var responseReaderTask = Task.Run(async () =>
                {
                    while (await call.ResponseStream.MoveNext())
                    {
                        var ack = call.ResponseStream.Current;
                        recordAcks.Add(ack);
                    }
                });

                foreach (Record record in records)
                {
                    await call.RequestStream.WriteAsync(record);
                }

                await call.RequestStream.CompleteAsync();
                await responseReaderTask;
            }

            // assert
            Assert.Equal(3, recordAcks.Count);
            Assert.Empty(
                recordAcks.Select(r => r.Error).Where(err => !string.IsNullOrWhiteSpace(err)).ToArray()
            );
            Assert.Empty(
                recordAcks.Select(r => r.CorrelationId).Where(cId => cId != "test_escape").ToArray()
            );

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }
        
        [Fact]
        public async Task WriteTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginMySQL.Plugin.Plugin())},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var configureRequest = new ConfigureWriteRequest
            {
                Form = new ConfigurationFormRequest
                {
                    DataJson = JsonConvert.SerializeObject(new ConfigureWriteFormData
                    {
                        StoredProcedure = "`test`.`UpsertIntoTestTable`",
                        GoldenRecordIdParam = "U_ID"
                    })
                }
            };

            var records = new List<Record>
            {
                new Record
                {
                    Action = Record.Types.Action.Upsert,
                    CorrelationId = "test",
                    RecordId = "record1",
                    DataJson = "{\"U_ID\":\"1\",\"U_NAME\":\"Test First\"}",
                }
            };

            var recordAcks = new List<RecordAck>();

            // act
            client.Connect(connectRequest);

            var configureResponse = client.ConfigureWrite(configureRequest);

            var prepareWriteRequest = new PrepareWriteRequest()
            {
                Schema = configureResponse.Schema,
                CommitSlaSeconds = 1000,
                DataVersions = new DataVersions
                {
                    JobId = "jobUnitTest",
                    ShapeId = "shapeUnitTest",
                    JobDataVersion = 1,
                    ShapeDataVersion = 1
                }
            };
            client.PrepareWrite(prepareWriteRequest);

            using (var call = client.WriteStream())
            {
                var responseReaderTask = Task.Run(async () =>
                {
                    while (await call.ResponseStream.MoveNext())
                    {
                        var ack = call.ResponseStream.Current;
                        recordAcks.Add(ack);
                    }
                });

                foreach (Record record in records)
                {
                    await call.RequestStream.WriteAsync(record);
                }

                await call.RequestStream.CompleteAsync();
                await responseReaderTask;
            }

            // assert
            Assert.Single(recordAcks);
            Assert.Equal("", recordAcks[0].Error);
            Assert.Equal("test", recordAcks[0].CorrelationId);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }
    }
}