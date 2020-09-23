using System;
using System.IO;
using System.Threading;
using Grpc.Core;
using Serilog;

namespace PluginMySQL.Helper
{
    public static class Logger
    {
        public enum LogLevel
        {
            Verbose,
            Debug,
            Info,
            Error,
            Off
        }

        private static string _logPrefix = "";
        private static string _fileName = @"plugin-mysql-log.txt";
        private static LogLevel _level = LogLevel.Info;

        /// <summary>
        /// Initializes the logger
        /// </summary>
        public static void Init()
        {
            // ensure log directory exists
            Directory.CreateDirectory("logs");
            
            // setup serilog
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Verbose()
                .Enrich.FromLogContext()
                .WriteTo.Async(
                    sinkConfig =>
                    {
                        sinkConfig.File(
                            $"logs/{_fileName}",
                            rollingInterval: RollingInterval.Day,
                            shared: true,
                            rollOnFileSizeLimit: true
                        );
                        sinkConfig.Console();
                    })
                .CreateLogger();
        }

        /// <summary>
        /// Closes the logger and flushes any pending messages in the buffer
        /// </summary>
        public static void CloseAndFlush()
        {
            Log.CloseAndFlush();
        }
        
        /// <summary>
        /// Deletes log file if it is older than 7 days
        /// </summary>
        public static void Clean()
        {
            if (File.Exists(_fileName))
            {
                if ((File.GetCreationTime(_fileName) - DateTime.Now).TotalDays > 7)
                {
                    File.Delete(_fileName);
                }
            }
        }

        /// <summary>
        /// Logging method for Verbose messages
        /// </summary>
        /// <param name="message"></param>
        public static void Verbose(string message)
        {
            if (_level > LogLevel.Verbose)
            {
                return;
            }
            
            GrpcEnvironment.Logger.Debug(message);
            
            // WriteLog(message);
            Log.Verbose(message);
        }
        
        /// <summary>
        /// Logging method for Debug messages
        /// </summary>
        /// <param name="message"></param>
        public static void Debug(string message)
        {
            if (_level > LogLevel.Debug)
            {
                return;
            }
            
            GrpcEnvironment.Logger.Debug(message);
            
            // WriteLog(message);
            Log.Debug(message);
        }
        /// <summary>
        /// Logging method for Info messages
        /// </summary>
        /// <param name="message"></param>
        public static void Info(string message)
        {
            if (_level > LogLevel.Info)
            {
                return;
            }
            
            GrpcEnvironment.Logger.Info(message);
            
            // WriteLog(message);
            Log.Information(message);
        }
        
        /// <summary>
        /// Logging method for Error messages
        /// </summary>
        /// <param name="exception"></param>
        /// <param name="message"></param>
        public static void Error(Exception exception, string message)
        {
            if (_level > LogLevel.Error)
            {
                return;
            }
            
            GrpcEnvironment.Logger.Error(exception, message);
            
            // WriteLog(message);
            Log.Error(exception, message);
        }
        
        /// <summary>
        /// Logging method for Error messages to the context
        /// </summary>
        /// <param name="message"></param>
        /// <param name="exception"></param>
        /// <param name="context"></param>
        public static void Error(Exception exception, string message, ServerCallContext context)
        {
            if (_level > LogLevel.Error)
            {
                return;
            }
            
            GrpcEnvironment.Logger.Error(exception, message);
            context.Status = new Status(StatusCode.Unknown, message);
            
            // WriteLog(message);
            Log.Error(exception, message);
        }

        /// <summary>
        /// Sets the log level 
        /// </summary>
        /// <param name="level"></param>
        public static void SetLogLevel(LogLevel level)
        {
            _level = level;
        }

        /// <summary>
        /// Sets a 
        /// </summary>
        /// <param name="logPrefix"></param>
        public static void SetLogPrefix(string logPrefix)
        {
            _logPrefix = logPrefix;
        }
    }
}