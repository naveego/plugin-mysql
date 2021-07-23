using System;
using System.Collections.Generic;
using PluginExasol.Helper;
using Xunit;

namespace PluginExasolTest.Helper
{
    public class SettingsTest
    {
        [Fact]
        public void ValidateValidTest()
        {
            // setup
            var settings = new Settings
            {
                Hostname = "123.456.789.0",
                Port = "8563",
                Username = "username",
                Password = "password"
            };

            // act
            settings.Validate();

            // assert
        }

        [Fact]
        public void ValidateNoHostNameTest()
        {
            // setup
            var settings = new Settings
            {
                Hostname = null,
                Port = "8563",
                Username = "username",
                Password = "password"
            };

            // act
            Exception e = Assert.Throws<Exception>(() => settings.Validate());

            // assert
            Assert.Contains("The Hostname property must be set", e.Message);
        }
        
        [Fact]
        public void ValidateNoPortTest()
        {
            // setup
            var settings = new Settings
            {
                Hostname = "123.456.789.0",
                Port = null,
                Username = "username",
                Password = "password"
            };

            // act
            Exception e = Assert.Throws<Exception>(() => settings.Validate());

            // assert
            Assert.Contains("The Port property must be set", e.Message);
        }
        
        [Fact]
        public void ValidateNoUsernameTest()
        {
            // setup
            var settings = new Settings
            {
                Hostname = "123.456.789.0",
                Port = "8563",
                Username = null,
                Password = "password"
            };

            // act
            Exception e = Assert.Throws<Exception>(() => settings.Validate());

            // assert
            Assert.Contains("The Username property must be set", e.Message);
        }
        
        [Fact]
        public void ValidateNoPasswordTest()
        {
            // setup
            var settings = new Settings
            {
                Hostname = "123.456.789.0",
                Port = "8563",
                Username = "username",
                Password = null
            };

            // act
            Exception e = Assert.Throws<Exception>(() => settings.Validate());

            // assert
            Assert.Contains("The Password property must be set", e.Message);
        }
        
        [Fact]
        public void GetConnectionStringTest()
        {
            // setup
            var settings = new Settings
            {
                Hostname = "123.456.789.0",
                Port = "8563",
                Username = "username",
                Password = "password"
            };

            // act
            var connString = settings.GetConnectionString();
            var connDbString = settings.GetConnectionString();

            // assert
            Assert.Equal("Server=123.456.789.0; Port=8563; Database=master; User=username; Password=password;", connString);
            Assert.Equal("Server=123.456.789.0; Port=8563; Database=otherdb; User=username; Password=password;", connDbString);
        }
    }
}