var http =              require('http')                                 ;
var util =              require('util')                                 ;
var db =                require('./mongox.js')                          ;
var fs =                require('fs')                                   ;
var json2csv =          require('nice-json2csv')                        ;
var col =               'egb'                                           ;
var evaldb =            'evalegb'                                       ;
var ecsv =              __dirname +'/csv/e.csv'                         ;
var gcsv =              __dirname +'/csv/g.csv'                         ;
var append_flag =       { flags: 'w' }                                  ;
var estream, gstream, c;


/*

csv will have duplicates
to remove duplicates from csv

awk '!a[$0]++' csv/e.csv > csv/r.csv
awk '!a[$0]++' csv/g.csv > csv/h.csv

head -n 50 csv/r.csv > csv/e.csv
head -n 50 csv/h.csv > csv/g.csv
mv csv/e.csv csv/r.csv
mv csv/g.csv csv/h.csv

*/


var headers_train = ["id","date","g1_nick","g1_flag","g1_race","g1_points","g1_coef","g2_nick","g2_flag","g2_race","g2_points","g2_coef","nested","pid","game","tourn","outcome"];
var headers_score = ["id","date","g1_nick","g1_flag","g1_race","g1_points","g1_coef","g2_nick","g2_flag","g2_race","g2_points","g2_coef","nested","pid","game","tourn"];

var t = { //find
    "ut": { "$gte": "2015-01-01 00:00:00" },
    "bets.winner": { "$gte": 0 }
};

var p = { //excludes
          
    //"_id":0,        
    //"ut":0, 
    "filters":0,        
    "action":0,                  
    "options":0,        
 
    //"bets.id":0,
    //"bets.gamer_1.points":0,        
    //"bets.gamer_2.points":0,                       
    //"bets.date":0,
    "bets.has_advantage":0,
    "bets.coef_draw":0,
    "bets.filter":0,
    "bets.updated_at":0,
    "bets.deleted":0,
    "bets.game_id":0,
    "bets.nf":0,
    "bets.horvath":0,
    "bets.ee":0,
    "bets.date_t":0,
    "bets.has_bet":0,
    "bets.with_advantage":0,
    "bets.is_nested_bet_last":0,
    
    //"bets.nb_arr.id":0,
    //"bets.nb_arr.parent_id":0,
    //"bets.nb_arr.gamer_1.points":0,        
    //"bets.nb_arr.gamer_2.points":0,                             
    //"bets.nb_arr.date":0,
    "bets.nb_arr.has_advantage":0,
    "bets.nb_arr.coef_draw":0,
    "bets.nb_arr.filter":0,
    "bets.nb_arr.updated_at":0,
    "bets.nb_arr.deleted":0,
    "bets.nb_arr.game_id":0,
    "bets.nb_arr.nf":0,
    "bets.nb_arr.horvath":0,
    "bets.nb_arr.ee":0,
    "bets.nb_arr.date_t":0,
    "bets.nb_arr.has_bet":0,
    "bets.nb_arr.with_advantage":0,
    "bets.nb_arr.is_nested_bet_last":0,
};
var s = {'ut':-1};
var l = 1;



/*

    entry 
    
    get -> create csvs -> reduce -> predict

*/
process.argv.forEach(function(val, index, array) {

    var s = val.split('=');    
    switch(s[0]){

        case "g":       getgames(); break;

        case "t":       traincsv(); break;
        case "s":       scorecsv(); break;            
        
        case 'x':   
            removeduplicates("awk '!a[$0]++' csv/e.csv > csv/r.csv");
            removeduplicates("awk '!a[$0]++' csv/g.csv > csv/h.csv");
            break;

        case "m":       ml(); break;
        
    }
    
});



/*

    fetch documents for train

*/
function traincsv(){

    estream = fs.createWriteStream(ecsv, append_flag);    
    estream.write( ''+ headers_train +"\n"  );    
    l = 1000;
    
    db.collection(col).find(t, p).limit(l).toArray(function(err, docs) {
        if (err) console.log(err);
                
console.log( docs.length ); 
console.log( docs[0].bets.length );

        if (docs) {                    
            docs.forEach(function(bet,i) {     
                
                bet.bets.forEach(function(doc,i) {

                    if(doc.winner > 0){                        
                        train(doc);
                        
                        if(doc.has_nested_bets){                                        
                            doc.nb_arr.forEach(function(nb,i) {            
                                if(nb.winner > 0){        
                                    train(nb);                            
                                }else{
                                    //score(nb);                            
                                }

                            });
                                
                        }
                        
                    }else{ 
                        //score(doc);    
                        
                        if(doc.has_nested_bets){                                        
                            doc.nb_arr.forEach(function(nb,i) {
            
                                if(nb.winner > 0){        
                                    train(nb);                            
                                }else{
                                    //score(nb);                            
                                }

                            });
                                
                        }
                        
                    }
    
                });     
                
                //finish
                if(i == docs.length-1){ 
                    console.log('done: train'); 
                    db.close();
                    estream.end();
                }
            });
        }

    }); 

}


/*

    fetch documents for score

*/
function scorecsv(){

    gstream = fs.createWriteStream(gcsv, append_flag);
    gstream.write( ''+ headers_score +"\n"  );        
    l = 1;
    
    db.collection(col).find(t, p).sort(s).limit(l).toArray(function(err, docs) {
        if (err) console.log(err);

console.log( docs.length ); 
console.log( docs[0].bets.length );

        if (docs) {                    
            docs.forEach(function(bet,i) {            
                bet.bets.forEach(function(doc,i) {

                    if(doc.winner > 0){                        
                        //train(doc);
                        
                        if(doc.has_nested_bets){                                        
                            doc.nb_arr.forEach(function(nb,i) {            
                                if(nb.winner > 0){        
                                    //train(nb);                            
                                }else{
                                    score(nb);                            
                                }

                            });
                                
                        }
                        
                    }else{ 
                        score(doc);    
                        
                        if(doc.has_nested_bets){                                        
                            doc.nb_arr.forEach(function(nb,i) {
            
                                if(nb.winner > 0){        
                                    //train(nb);                            
                                }else{
                                    score(nb);                            
                                }

                            });
                                
                        }                        
                        
                    }
    
                });            

                //finish
                if(i == docs.length-1){ 
                    console.log('done: score'); 
                    db.close();
                    gstream.end();
                }

            });
            
        }

    }); 
    
    

}


/*

    write train csv

*/
function train(doc){

    c = {
            
        id:         doc.id             ||0,
        date:       doc.date           ||0,
        
        g1_nick:    doc.gamer_1.nick   ||0,
        g1_flag:    doc.gamer_1.flag   ||0,
        g1_race:    doc.gamer_1.race   ||0,                        
        g1_points:  doc.gamer_1.points ||0,   
        g1_coef:    doc.coef_1         ||0,
        
        g2_nick:    doc.gamer_2.nick   ||0,
        g2_flag:    doc.gamer_2.flag   ||0,
        g2_race:    doc.gamer_2.race   ||0,        
        g2_points:  doc.gamer_2.points ||0,   
        g2_coef:    doc.coef_2         ||0,
        
        nested:     doc.nested_bets_count ||0,
        pid:        doc.parent_id      ||0,
        game:       doc.game           ||0,
        tourn:      doc.tourn          ||0,
        outcome:    doc.gamer_2.win    //gamer 2 is taken as the outcome
        
    };

    estream.write( json2csv.convert(c, headers_train, true) + "\n" );

}


/*

    write score csv

*/
function score(doc){

    c = {
            
        id:         doc.id             ||0,
        date:       doc.date           ||0,
        
        g1_nick:    doc.gamer_1.nick   ||0,
        g1_flag:    doc.gamer_1.flag   ||0,
        g1_race:    doc.gamer_1.race   ||0,                        
        g1_points:  doc.gamer_1.points ||0,   
        g1_coef:    doc.coef_1         ||0,
        
        g2_nick:    doc.gamer_2.nick   ||0,
        g2_flag:    doc.gamer_2.flag   ||0,
        g2_race:    doc.gamer_2.race   ||0,        
        g2_points:  doc.gamer_2.points ||0,   
        g2_coef:    doc.coef_2         ||0,
        
        nested:     doc.nested_bets_count ||0,
        pid:        doc.parent_id      ||0,
        game:       doc.game           ||0,
        tourn:      doc.tourn          ||0        
        
    };

    gstream.write( json2csv.convert(c, headers_score, true) + "\n" );

}



/*

    fetch current games

*/
function getgames() {

    var request =   require('request');
    var zlib =      require('zlib');

    var headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Encoding': 'gzip, deflate, sdch',
        'Accept-Language': 'en-US,en;q=0.8',
        'Connection': 'keep-alive',
        'Host': 'egb.com',
        'Referer': 'http://egb.com/tables',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36'
    };

    var key =   'modules_tables_update_UpdateTableBets';
    var act =   'UpdateTableBets';
    var ajax =  'update';
    var ind =   'tables';
    var type =  'modules';
    var fg =    1;
    var st =    0; 
    var ut =    0; 

    var options = {
        url: 'http://egb.com/ajax.php' + 
            '?key='     + key + 
            '&act='     + act + 
            '&ajax='    + ajax + 
            '&fg='      + fg + 
            '&ind='     + ind + 
            '&st='      + st + 
            '&type='    + type + 
            '&ut='      + ut,
        headers: headers
    };


    var requestWithEncoding = function(options, callback) {
        var req = request.get(options);

        req.on('response', function(res) {

            //console.log(res.statusCode);
            //console.log( res.headers );             

            var chunks = [];
            res.on('data', function(chunk) {
                chunks.push(chunk);                
            });

            res.on('end', function() {
                var buffer = Buffer.concat(chunks);
                var encoding = res.headers['content-encoding'];
                if (encoding == 'gzip') {
                    zlib.gunzip(buffer, function(err, decoded) {

                        callback(err, decoded && decoded.toString());
                    });
                }
                else if (encoding == 'deflate') {
                    zlib.inflate(buffer, function(err, decoded) {
                        callback(err, decoded && decoded.toString());
                    });
                }
                else {
                    callback(null, buffer.toString());
                }
            });
        });

        req.on('error', function(err) {
            callback(err);
        });
    };


    requestWithEncoding(options, function(err, data) {
        if (err) console.log(err);

        data = JSON.parse(data);
        db.collection(col).insert(
            data
        );

        console.log('done');

        setTimeout( (function(){ process.exit(1) }) , 2*60000 );
        
    });


}



/*

    ml
    
*/
function ml() {

    var apikey, workspace, services, score, payload, scored, train, train_url, score_url;
    var scorejs =       require('./score.js')                       ;
    var Converter =     require("csvtojson").core.Converter         ;
    var scoredb =       'edb'                                       ;
    var evaldb =        'evalegb'                                   ;
    var debug =         0                                           ;
    var timeoutsecs =   1 * 60000                                   ;

    apikey = "NPCn//58b28w5iw0T/AMmmmPC5skC33bAKmPVttXLpDRGymFHDZ4I0rCmsWGzwnlVIp7fWZfsuqFubmjWiDEhw==";
    workspace = "0360466b83b04998ab86ea69dbcec5bf";
    services = "5d951bca63824cfe9c9d88fe67633e31";
    score = "jobs";

    payload = { 
        "GlobalParameters": {}, 
        "Outputs": { 
            "res": { 
                "ConnectionString": "DefaultEndpointsProtocol=https;AccountName=mystorageacct;AccountKey=Dx9WbMIThAvXRQWap/aLnxT9LV5txxw==", 
                "RelativeLocation": "mycontainer/resresults.csv" 
            } 
        } 
    };
    
    var scorex = {

        init: function() {

            //score init
            scorejs.post(apikey, workspace, services, score, payload, function(jobid) {

                //score running
                if (jobid) {
                    console.log(jobid);
                    scorex.loopa(apikey, workspace, services, score, payload, jobid);

                }

            });

        }


        ,loopa: function(apikey, workspace, services, score, payload, jobid) {

            function myLoop(apikey, workspace, services, score, payload, jobid) {

                scorejs.get(apikey, workspace, services, score, payload, jobid, function(data) {

                    if (data) {

                        console.log('status: ' + data.StatusCode);
                        if (debug) console.log(data);

                        if (data.StatusCode === 0 || data.StatusCode == "Not started" || data.StatusCode == "NotStarted") {
                            //Not started 

                            setTimeout(function() {
                                scorex.loopa(apikey, workspace, services, score, payload, jobid);
                            }, timeoutsecs);

                        }
                        else if (data.StatusCode === 1 || data.StatusCode == "Running") {
                            //Running

                            setTimeout(function() {
                                scorex.loopa(apikey, workspace, services, score, payload, jobid);
                            }, timeoutsecs);

                        }
                        else if (data.StatusCode === 2 || data.StatusCode == "Failed") {
                            //Failed
                        }
                        else if (data.StatusCode === 3 || data.StatusCode == "Cancelled") {
                            //Cancelled
                        }
                        else if (data.StatusCode === 4 || data.StatusCode == "Finished") {
                            //Finished

                            scorex.finished(apikey, workspace, services, score, payload, jobid, data);

                        }

                    }

                });


            }

            //initiate loop
            myLoop(apikey, workspace, services, score, payload, jobid);
        }


        ,finished: function(apikey, workspace, services, score, payload, jobid, data) {

            var result = scorejs.batch(
                data.Results.res.BaseLocation + data.Results.res.RelativeLocation + data.Results.res.SasBlobToken,
                function(csv) {

                    var csvConverter = new Converter();

                    //delete batch job once complete
                    csvConverter.on("end_parsed", function(jsonObj) {
                        scorejs.delete(apikey, workspace, services, score, {}, jobid, function(data) {


                            if (debug) console.log(data);
                            return 0;
                        });
                    });


                    if (csv) {
                        csvConverter.fromString(csv, function(err, jsonObj) {

                            if (debug) console.log(jsonObj);

                            if (err)
                                console.log('api exp csv conv: ', err);

                            try {
                                db.collection(evaldb).insert({

                                    'date':(new Date()).getTime(),
                                    
                                    "eval": {
                                        "acc": jsonObj[0]["Accuracy"].toFixed(2),
                                        "prec": jsonObj[0]["Precision"].toFixed(2),
                                        "recall": jsonObj[0]["Recall"].toFixed(2),
                                        "fscore": jsonObj[0]["F-Score"].toFixed(2),
                                        "auc": jsonObj[0]["AUC"].toFixed(2),
                                        "all": jsonObj[0]["Average Log Loss"].toFixed(2),
                                        "tll": jsonObj[0]["Training Log Loss"].toFixed(2),
                                    },

                                    "xval": {
                                        "acc": jsonObj[1]["Accuracy"].toFixed(2),
                                        "prec": jsonObj[1]["Precision"].toFixed(2),
                                        "recall": jsonObj[1]["Recall"].toFixed(2),
                                        "fscore": jsonObj[1]["F-Score"].toFixed(2),
                                        "auc": jsonObj[1]["AUC"].toFixed(2),
                                        "all": jsonObj[1]["Average Log Loss"].toFixed(2),
                                        "tll": jsonObj[1]["Training Log Loss"].toFixed(2),
                                    }
                                });
                            }
                            catch (e) {
                                console.log('error insert: ', e);
                            }

                            jsonObj.forEach(function(doc) {

                                try {
                                    db.collection(scoredb).insert({                                        
                                        '_id':        doc.id                ||0,                                    
                                        'date':       doc.date              ||0, 
                                        'g1_nick':    doc.g1_nick           ||0,     
                                        'g2_nick':    doc.g2_nick           ||0,                                        
                                        'pid':        doc.pid               ||0,
                                        'nested':     doc.nested            ||0,
                                        'game':       doc.game              ||0,
                                        'tourn':      doc.tourn             ||0,
                                        'label':      doc["Scored Probabilities"].toFixed(4) > jsonObj[0]["AUC"].toFixed(4),
                                        'prob':       doc["Scored Probabilities"].toFixed(4),
                                        'auc':        jsonObj[0]["AUC"].toFixed(4)
                                    });
                                }
                                catch (e) {
                                    console.log('error insert: ', e);
                                }

                            });

                        });

                        console.log('insert', jobid);

                    }

                }
            );

        }

    };

    new scorex.init();

}


/*

    remove duplicates

*/
function removeduplicates(rmd){
    
    var sys = require('sys');
    var exec = require('child_process').exec;
    var child;
     
    child = exec(rmd, function (error, stdout, stderr) {
        if(stdout) sys.print('stdout: ' + stdout);
        if(stderr) sys.print('stderr: ' + stderr);
        if (error !== null) {
            console.log('exec error: ' + error);
        }
              
        console.log('cpc'); 
                        
    });
        
}



/*

    helper fundtions

*/
var flattenObject = function(ob) {
    var toReturn = {};

    for (var i in ob) {
        if (!ob.hasOwnProperty(i)) continue;

        if ((typeof ob[i]) == 'object') {
            var flatObject = flattenObject(ob[i]);
            for (var x in flatObject) {
                if (!flatObject.hasOwnProperty(x)) continue;

                toReturn[i + '.' + x] = flatObject[x];
            }
        }
        else {
            toReturn[i] = ob[i];
        }
    }
    return toReturn;
};

RegExp.prototype.execAll = function(string) {
    var match = null;
    var matches = [];
    while (match = this.exec(string)) {
        var matchArray = [];
        for (var i in match) {
            if (parseInt(i, 10) == i) {
                matchArray.push(match[i]);
            }
        }
        matches.push(matchArray);
    }
    return matches;
};

function todays_date(ago) {

    var today =     new Date();
    var offset =    10;                     //timezone aus/syd +10
    var utc =       today.getTime() + (today.getTimezoneOffset() * 60000);
    today =         new Date(utc + (3600000 * offset));
    var dd =        today.getDate();
    var mm =        today.getMonth() + 1;   //January is 0!
    var yyyy =      today.getFullYear();

    var todayD =    today.getDate() - ago;  // 2 weeks ago
    today.setDate(todayD); 
    var iso =       today.toISOString();

    return {"day":dd, "month":mm, "year":yyyy, "iso":iso};

}
