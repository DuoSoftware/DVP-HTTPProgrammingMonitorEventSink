var amqp = require("amqp");
var logger = require('dvp-common/LogHandler/CommonLogHandler.js').logger;
var dbModel = require('dvp-dbmodels');
var config = require('config');
var redis = require('ioredis');
var Mailer=require('./Mailer.js');

var FileUploaded='SYS:HTTPPROGRAMMING:FILEUPLOADED';
var DataError='SYS:HTTPPROGRAMMING:DATAERROR';
var HttpError='SYS:HTTPPROGRAMMING:HTTPERROR';

if(config.evtConsumeType === 'amqp')
{
    var amqpConState = 'CLOSED';

    // var connection = amqp.createConnection({ host: rmqIp, port: rmqPort, login: rmqUser, password: rmqPassword});

    var ips = [];
    if(config.RabbitMQ.ip) {
        ips = config.RabbitMQ.ip.split(",");
    }


    var connection = amqp.createConnection({
        //url: queueHost,
        host: ips,
        port: config.RabbitMQ.port,
        login: config.RabbitMQ.user,
        password: config.RabbitMQ.password,
        vhost: config.RabbitMQ.vhost,
        noDelay: true,
        heartbeat:10
    }, {
        reconnect: true,
        reconnectBackoffStrategy: 'linear',
        reconnectExponentialLimit: 120000,
        reconnectBackoffTime: 1000
    });

    //logger.debug('[DVP-EventMonitor.handler] - [%s] - AMQP Creating connection ' + rmqIp + ' ' + rmqPort + ' ' + rmqUser + ' ' + rmqPassword);

    connection.on('connect', function()
    {
        amqpConState = 'CONNECTED';
        logger.debug('[DVP-EventService.AMQPConnection] - [%s] - AMQP Connection CONNECTED');
    });

    connection.on('ready', function()
    {
        amqpConState = 'READY';

        logger.debug('[DVP-EventService.AMQPConnection] - [%s] - AMQP Connection READY');

        connection.queue('DVPHTTPEVENTS', {durable: true, autoDelete: false}, function (q) {
            q.bind('#');

            // Receive messages
            q.subscribe(function (message) {


//pattern,MsgTyp, message

                var reqId = '';

                try {
                    reqId = uuid.v1();
                }
                catch (ex) {

                }

                logger.debug('[DVP-HTTPProgrammingMonitorAPI] - [%s]  - Redis client started ', reqId);
                var Jobj = message;
                //JSON.parse(message);

                var MsgTyp = message.MType;

                if (MsgTyp == FileUploaded) {
                    logger.debug('[DVP-HTTPProgrammingMonitorAPI] - [%s]  - Message type :  %s', reqId, FileUploaded);
                    Mailer.GetAppDeveloperMail(Jobj.APPID, reqId, function (err, res) {
                        if (err) {
                            console.error(err);
                            logger.error('[DVP-HTTPProgrammingMonitorAPI] - [%s]  - Redis client starting error ', reqId, err);
                        }
                        else {
                            logger.debug('[DVP-HTTPProgrammingMonitorAPI] - [%s]  - Mail sending starts for Application  :  %s', reqId, Jobj.APPID);
                            Mailer.AlertSender(res, 'Application Upload Notification', 'Your new application ' + Jobj.DisplayName + ' is successfully uploaded and Application ID will be ' + Jobj.APPID, reqId, function (errAlert, ResAlert) {
                                if (errAlert) {
                                    console.error(errAlert);
                                }
                                else {
                                    console.log(ResAlert);
                                }
                            });
                        }
                    })
                }
                else {
                    if (MsgTyp == DataError) {
                        logger.debug('[DVP-HTTPProgrammingMonitorAPI] - [%s]  - Message type :  %s', reqId, DataError);
                        GetErrorCount(Jobj.APPID, reqId, function (errCount, resCount) {
                            if (errCount) {
                                //console.error(errCount);
                            }
                            else {
                                var Count = parseInt(resCount);
                                logger.debug('[DVP-HTTPProgrammingMonitorAPI] - [%s]  - %s Errors found in Application  :  %s', reqId, Count, APPID);
                                if (Count >= 9) {
                                    Mailer.GetAppDeveloperMail(Jobj.APPID, reqId, function (err, res) {
                                        if (err) {
                                            console.error(err);
                                        }
                                        else {
                                            logger.debug('[DVP-HTTPProgrammingMonitorAPI] - [%s]  - Mail sending starts for Application  :  %s', reqId, Jobj.APPID);
                                            Mailer.AlertSender(res, 'Error Notification', 'Your Apllication ' + Jobj.APPID + ' is come up with 10 errors.Please check', reqId, function (errAlert, ResAlert) {
                                                if (errAlert) {
                                                    console.error(errAlert);
                                                }
                                                else {

                                                }
                                            });
                                        }
                                    })
                                }


                                try {
                                    var NewErrobj = dbModel.ApplicationErrors
                                        .build(
                                            {
                                                VoiceAppID: Jobj.VoiceAppID,
                                                Code: Jobj.Code,
                                                Message: Jobj.Description,
                                                SessionID: Jobj.SessionID,
                                                URL: Jobj.URL
                                            }
                                        );
                                    logger.debug('[DVP-HTTPProgrammingMonitorAPI] - [%s]  - New DataError details of  Application  :  %s - Data - %s', reqId, APPID, JSON.stringify(NewErrobj));
                                }
                                catch (ex) {
                                    logger.error('[DVP-HTTPProgrammingMonitorAPI] - [%s]  - Exception occurred while creating New DataError details of  Application  :  %s ', reqId, Jobj.APPID, ex);
                                    console.error(ex);
                                }

                                try {
                                    NewErrobj.save().then(function (resultSave) {


                                        logger.debug('[DVP-HTTPProgrammingMonitorAPI] - [%s] - [PGSQL] -  New DataError details of  Application  :  %s is successfully saved', reqId, Jobj.APPID);
                                        console.log(resultSave);

                                    }).catch(function (errSave) {

                                        logger.error('[DVP-HTTPProgrammingMonitorAPI] - [%s] - [PGSQL] - Error occurred while saving New DataError details of  Application  :  %s ', reqId, Jobj.APPID, err);
                                        console.error(errSave);

                                    });
                                }
                                catch (ex) {
                                    logger.error('[DVP-HTTPProgrammingMonitorAPI] - [%s] - [PGSQL] - Exception occurred when saving New DataError details of  Application  :  %s ', reqId, Jobj.APPID, ex);
                                    console.error(ex);
                                }
                            }

                        });
                    }

                    else if (MsgTyp == HttpError) {
                        logger.debug('[DVP-HTTPProgrammingMonitorAPI] - [%s]  - Message type :  %s', reqId, HttpError);
                        GetErrorCount(Jobj.APPID, reqId, function (errCount, resCount) {
                            if (errCount) {
                                console.error(errCount);
                            }
                            else {
                                var Count = parseInt(resCount);
                                logger.debug('[DVP-HTTPProgrammingMonitorAPI] - [%s]  - %s Errors found in Application  :  %s', reqId, Count, Jobj.APPID);
                                if (Count >= 9) {
                                    Mailer.GetAppDeveloperMail(Jobj.APPID, reqId, function (err, res) {
                                        if (err) {
                                            console.error(err);
                                        }
                                        else {
                                            logger.debug('[DVP-HTTPProgrammingMonitorAPI] - [%s]  - Mail sending starts for Application  :  %s', reqId, Jobj.APPID);
                                            Mailer.AlertSender(res, 'Error Notification', 'Your Application ' + Jobj.APPID + ' is come up with 10 errors.Please check', reqId, function (errAlert, ResAlert) {
                                                if (errAlert) {
                                                    console.error(errAlert);
                                                }
                                                else {

                                                }


                                            });
                                        }
                                    });
                                }


                                try {
                                    var NewErrobj = dbModel.ApplicationErrors
                                        .build(
                                            {
                                                VoiceAppID: Jobj.VoiceAppID,
                                                Code: Jobj.Code,
                                                Message: Jobj.Description,
                                                SessionID: Jobj.SessionID,
                                                URL: Jobj.URL
                                            }
                                        );
                                    logger.debug('[DVP-HTTPProgrammingMonitorAPI] - [%s]  - New HttpError details of  Application  :  %s - Data - %s', reqId, Jobj.APPID, JSON.stringify(NewErrobj));
                                }
                                catch (ex) {
                                    logger.error('[DVP-HTTPProgrammingMonitorAPI] - [%s]  - Exception occurred while creating New HttpError details of  Application  :  %s ', reqId, Jobj.APPID, ex);
                                }

                                try {
                                    NewErrobj.save().then(function (resultSave) {


                                        logger.debug('[DVP-HTTPProgrammingMonitorAPI] - [%s] - [PGSQL] -  New HttpError details of  Application  :  %s is successfully saved', reqId, Jobj.APPID);
                                        console.log(resultSave);

                                    }).catch(function (errSave) {


                                        logger.error('[DVP-HTTPProgrammingMonitorAPI] - [%s] - [PGSQL] - Error occurred while saving New HttpError details of  Application  :  %s ', reqId, Jobj.APPID, errSave);
                                        console.error(errSave);

                                    });
                                }
                                catch (ex) {
                                    logger.error('[DVP-HTTPProgrammingMonitorAPI] - [%s] - [PGSQL] - Exception occurred when saving New HttpErrory details of  Application  :  %s ', reqId, Jobj.APPID, ex);
                                    console.error(ex);
                                }
                            }

                        });
                    }
                }


            });

        });
    });

    connection.on('error', function(e)
    {
        logger.error('[DVP-EventMonitor.handler] - [%s] - AMQP Connection ERROR', e);
        amqpConState = 'CLOSE';
    });

}
else
{
    var redisip = config.Redis.ip;
    var redisport = config.Redis.port;
    var redispass = config.Redis.password;
    var redismode = config.Redis.mode;
    var redisdb = config.Redis.db;



    var redisSetting =  {
        port:redisport,
        host:redisip,
        family: 4,
        password: redispass,
        db: redisdb,
        retryStrategy: function (times) {
            var delay = Math.min(times * 50, 2000);
            return delay;
        },
        reconnectOnError: function (err) {

            return true;
        }
    };

    if(redismode == 'sentinel'){

        if(config.Redis.sentinels && config.Redis.sentinels.hosts && config.Redis.sentinels.port && config.Redis.sentinels.name){
            var sentinelHosts = config.Redis.sentinels.hosts.split(',');
            if(Array.isArray(sentinelHosts) && sentinelHosts.length > 2){
                var sentinelConnections = [];

                sentinelHosts.forEach(function(item){

                    sentinelConnections.push({host: item, port:config.Redis.sentinels.port})

                })

                redisSetting = {
                    sentinels:sentinelConnections,
                    name: config.Redis.sentinels.name,
                    password: redispass
                }

            }else{

                console.log("No enough sentinel servers found .........");
            }

        }
    }

    var redisClient = undefined;

    if(redismode != "cluster") {
        redisClient = new redis(redisSetting);
    }else{

        var redisHosts = redisip.split(",");
        if(Array.isArray(redisHosts)){


            redisSetting = [];
            redisHosts.forEach(function(item){
                redisSetting.push({
                    host: item,
                    port: redisport,
                    family: 4,
                    password: redispass});
            });

            var redisClient = new redis.Cluster([redisSetting]);

        }else{

            redisClient = new redis(redisSetting);
        }


    }

    redisClient.on('error', function (err) {
        console.log('Error '.red, err);
    });

    redisClient.psubscribe("SYS:HTTPPROGRAMMING:*");


    redisClient.on('pmessage', function (pattern,MsgTyp, message) {
        var reqId='';

        try
        {
            reqId = uuid.v1();
        }
        catch(ex)
        {

        }

        logger.debug('[DVP-HTTPProgrammingMonitorAPI] - [%s]  - Redis client started ',reqId);
        var Jobj=JSON.parse(message);

        if(MsgTyp == FileUploaded )
        {
            logger.debug('[DVP-HTTPProgrammingMonitorAPI] - [%s]  - Message type :  %s',reqId,FileUploaded);
            Mailer.GetAppDeveloperMail(Jobj.APPID,reqId,function(err,res)
            {
                if(err)
                {
                    console.error(err);
                    logger.error('[DVP-HTTPProgrammingMonitorAPI] - [%s]  - Redis client starting error ',reqId,err);
                }
                else
                {
                    logger.debug('[DVP-HTTPProgrammingMonitorAPI] - [%s]  - Mail sending starts for Application  :  %s',reqId,Jobj.APPID);
                    Mailer.AlertSender(res,'Application Upload Notification','Your new application '+Jobj.DisplayName+ ' is successfully uploaded and Application ID will be '+Jobj.APPID,reqId,function(errAlert,ResAlert)
                    {
                        if(errAlert)
                        {
                            console.error(errAlert);
                        }
                        else
                        {
                            console.log(ResAlert);
                        }
                    });
                }
            })
        }
        else
        {
            if(MsgTyp == DataError)
            {
                logger.debug('[DVP-HTTPProgrammingMonitorAPI] - [%s]  - Message type :  %s',reqId,DataError);
                GetErrorCount(Jobj.APPID,reqId,function(errCount,resCount)
                {
                    if(errCount)
                    {
                        //console.error(errCount);
                    }
                    else
                    {
                        var Count=parseInt(resCount);
                        logger.debug('[DVP-HTTPProgrammingMonitorAPI] - [%s]  - %s Errors found in Application  :  %s',reqId,Count,APPID);
                        if(Count>=9)
                        {
                            Mailer.GetAppDeveloperMail(Jobj.APPID,reqId,function(err,res)
                            {
                                if(err)
                                {
                                    console.error(err);
                                }
                                else
                                {
                                    logger.debug('[DVP-HTTPProgrammingMonitorAPI] - [%s]  - Mail sending starts for Application  :  %s',reqId,Jobj.APPID);
                                    Mailer.AlertSender(res,'Error Notification','Your Apllication '+Jobj.APPID+' is come up with 10 errors.Please check',reqId,function(errAlert,ResAlert)
                                    {
                                        if(errAlert)
                                        {
                                            console.error(errAlert);
                                        }
                                        else
                                        {
                                            try {
                                                var NewErrobj = dbModel.ApplicationErrors
                                                    .build(
                                                        {
                                                            VoiceAppID: Jobj.VoiceAppID,
                                                            Code: Jobj.Code,
                                                            Message: Jobj.Description,
                                                            SessionID: Jobj.SessionID,
                                                            URL: Jobj.URL
                                                        }
                                                    );
                                                logger.debug('[DVP-HTTPProgrammingMonitorAPI] - [%s]  - New DataError details of  Application  :  %s - Data - %s',reqId,APPID,JSON.stringify(NewErrobj));
                                            }
                                            catch (ex)
                                            {
                                                logger.error('[DVP-HTTPProgrammingMonitorAPI] - [%s]  - Exception occurred while creating New DataError details of  Application  :  %s ',reqId,Jobj.APPID,ex);
                                                console.error(ex);
                                            }

                                            try {
                                                NewErrobj.save().then(function (resultSave) {


                                                    logger.debug('[DVP-HTTPProgrammingMonitorAPI] - [%s] - [PGSQL] -  New DataError details of  Application  :  %s is successfully saved', reqId, Jobj.APPID);
                                                    console.log(resultSave);

                                                }).catch(function (errSave) {

                                                    logger.error('[DVP-HTTPProgrammingMonitorAPI] - [%s] - [PGSQL] - Error occurred while saving New DataError details of  Application  :  %s ', reqId, Jobj.APPID, err);
                                                    console.error(errSave);

                                                });
                                            }
                                            catch(ex)
                                            {
                                                logger.error('[DVP-HTTPProgrammingMonitorAPI] - [%s] - [PGSQL] - Exception occurred when saving New DataError details of  Application  :  %s ',reqId,Jobj.APPID,ex);
                                                console.error(ex);
                                            }
                                        }
                                    });
                                }
                            })
                        }
                    }

                })
            }

            else if(MsgTyp == HttpError )
            {
                logger.debug('[DVP-HTTPProgrammingMonitorAPI] - [%s]  - Message type :  %s',reqId,HttpError);
                GetErrorCount(Jobj.APPID,reqId,function(errCount,resCount)
                {
                    if(errCount)
                    {
                        console.error(errCount);
                    }
                    else
                    {
                        var Count=parseInt(resCount);
                        logger.debug('[DVP-HTTPProgrammingMonitorAPI] - [%s]  - %s Errors found in Application  :  %s',reqId,Count,APPID);
                        if(Count>=9)
                        {
                            Mailer.GetAppDeveloperMail(Jobj.APPID,reqId,function(err,res)
                            {
                                if(err)
                                {
                                    console.error(err);
                                }
                                else
                                {
                                    logger.debug('[DVP-HTTPProgrammingMonitorAPI] - [%s]  - Mail sending starts for Application  :  %s',reqId,Jobj.APPID);
                                    Mailer.AlertSender(res,'Error Notification','Your Application '+Jobj.APPID+' is come up with 10 errors.Please check',reqId,function(errAlert,ResAlert)
                                    {
                                        if(errAlert)
                                        {
                                            console.error(errAlert);
                                        }
                                        else
                                        {
                                            try {
                                                var NewErrobj = dbModel.ApplicationErrors
                                                    .build(
                                                        {
                                                            VoiceAppID: Jobj.VoiceAppID,
                                                            Code: Jobj.Code,
                                                            Message: Jobj.Description,
                                                            SessionID: Jobj.SessionID,
                                                            URL: Jobj.URL
                                                        }
                                                    );
                                                logger.debug('[DVP-HTTPProgrammingMonitorAPI] - [%s]  - New HttpError details of  Application  :  %s - Data - %s',reqId,APPID,JSON.stringify(NewErrobj));
                                            }
                                            catch (ex)
                                            {
                                                logger.error('[DVP-HTTPProgrammingMonitorAPI] - [%s]  - Exception occurred while creating New HttpError details of  Application  :  %s ',reqId,APPID,ex);
                                            }

                                            try {
                                                NewErrobj.save().then(function (resultSave) {

                                                    logger.debug('[DVP-HTTPProgrammingMonitorAPI] - [%s] - [PGSQL] -  New HttpError details of  Application  :  %s is successfully saved', reqId, Jobj.APPID);
                                                    console.log(resultSave);

                                                }).catch(function (errSave) {

                                                    logger.error('[DVP-HTTPProgrammingMonitorAPI] - [%s] - [PGSQL] - Error occurred while saving New HttpError details of  Application  :  %s ', reqId, Jobj.APPID, err);
                                                    console.error(errSave);

                                                });
                                            }
                                            catch(ex)
                                            {
                                                logger.error('[DVP-HTTPProgrammingMonitorAPI] - [%s] - [PGSQL] - Exception occurred when saving New HttpErrory details of  Application  :  %s ',reqId,Jobj.APPID,ex);
                                                console.error(ex);
                                            }
                                        }
                                    });
                                }
                            })
                        }
                    }

                });
            }
        }

    });


}


function GetErrorCount(AppID,reqId,callback)
{
    try {
        dbModel.ApplicationErrors.count({where: [{VoiceAppID: AppID}]}).then(function (ErrObj) {

            logger.debug('[DVP-HTTPProgrammingMonitorAPI.ErrorCount] - [%s] - [PGSQL] - Records found for ApplicationErrors of Application %s', reqId, AppID, JSON.stringify(ErrObj));
            callback(undefined, JSON.stringify(ErrObj));

        }).catch(function (Err) {

            logger.debug('[DVP-HTTPProgrammingMonitorAPI.ErrorCount] - [%s] - [PGSQL] -  Error occurred while searching ApplicationErrors of Application %s', reqId, AppID, err);
            callback(Err, undefined);

        });
    }
    catch(ex)
    {
        logger.debug('[DVP-HTTPProgrammingMonitorAPI.ErrorCount] - [%s]  - Exception in method starting : GetErrorCount of Application %s',reqId,AppID);
        callback(ex,undefined);
    }
}
