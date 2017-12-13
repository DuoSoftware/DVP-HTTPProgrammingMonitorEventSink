/**
 * Created by pawan on 4/27/2015.
 */

var DbConn = require('dvp-dbmodels');
var logger = require('dvp-common/LogHandler/CommonLogHandler.js').logger;

var nodemailer = require('nodemailer');

// create reusable transporter object using SMTP transport

var transporter = nodemailer.createTransport({
    service: 'Gmail',
    auth: {
        user: 'pawan@duosoftware.com',
        pass: 'apss'
    }
});


function AlertSender(To,Subject,body,reqId,callback)
{
    logger.debug('[DVP-HTTPProgrammingMonitorAPI.AlertSender] - [%s] - [MAIL] - AlertSender starts :  ',reqId);
    var mailOptions = {
        from: 'Duo Voice', // sender address
        to: To, // list of receivers
        subject: Subject, // Subject line
        text: body // plaintext body
        // html body
    };
    logger.debug('[DVP-HTTPProgrammingMonitorAPI.AlertSender] - [%s] - [MAIL] - Mail options %s :  ',reqId,JSON.stringify(mailOptions));
    try {


        transporter.sendMail(mailOptions, function (error, info) {
            if (error) {
                logger.error('[DVP-HTTPProgrammingMonitorAPI.AlertSender] - [%s] - [MAIL] - Mail sending failed %s :  ',reqId,error);
                callback(err, undefined);
            } else {
                logger.debug('[DVP-HTTPProgrammingMonitorAPI.AlertSender] - [%s] - [MAIL] - Mail sending succeeded %s :  ',reqId,info.response);
                callback(undefined, info.response);
            }
        });
    }
    catch(ex)
    {
        logger.error('[DVP-HTTPProgrammingMonitorAPI.AlertSender] - [%s] - [MAIL] - Exception occurred while stating mail sending function   :  ',reqId,ex);
        callback(ex, undefined);
    }

}

function GetAppDeveloperMail(AppID,reqId,callback)
{
    logger.debug('[DVP-HTTPProgrammingMonitorAPI.GetAppDeveloperMail] - [%s]  - Application Developer mail selection starts for Application:  %s',reqId,AppID);
    try {
        DbConn.Application.find({
            where: {id: AppID},
            include: [{model: DbConn.AppDeveloper, as: "AppDeveloper"}]
        }).complete(function (err, result) {
            if (err) {
                logger.error('[DVP-HTTPProgrammingMonitorAPI.GetAppDeveloperMail] - [%s]  - Error occurred while searching Developer Mail of the Application:  %s',reqId,AppID,err);

                callback(err, undefined);
            }
            else {
                if (result) {
                    logger.debug('[DVP-HTTPProgrammingMonitorAPI.GetAppDeveloperMail] - [%s]  - Developer Mail of the Application:  %s found',reqId,JSON.stringify(result));
                    callback(undefined, result.AppDeveloper.Email);
                }
                else {
                    logger.error('[DVP-HTTPProgrammingMonitorAPI.GetAppDeveloperMail] - [%s]  - No Developer Mail of the Application:  %s found',reqId,AppID);
                    callback("Null returned", undefined);
                }
            }
        });
    }
    catch(ex)
    {
        logger.error('[DVP-HTTPProgrammingMonitorAPI.GetAppDeveloperMail] - [%s]  - Exception occurred when starting Developer mail service',reqId,ex);
    }
}

function GetErrorCount(AppID,callback)
{
    DbConn.ApplicationErrors.count({where:{VoiceAppID:AppID}}).complete(function(err,result)
    {
        if(err)
        {
            callback(err,undefined);
        }
        else
        {
            callback(undefined,result);
        }
    })
}


module.exports.AlertSender = AlertSender;
module.exports.GetAppDeveloperMail = GetAppDeveloperMail;
module.exports.GetErrorCount = GetErrorCount;




