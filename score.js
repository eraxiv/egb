var http =  require('https');
var debug = 0;


module.exports =  {

  post: function(apikey, workspace, services, job, payload, callback) {

    //stringify to get length
    payload = JSON.stringify(payload);


    if(debug)
    console.log(payload); 


    //setup options
    var post_options = {
      host: 'ussouthcentral.services.azureml.net',
      port: '443',
      path: '/workspaces/' + workspace + '/services/' + services + '/' + job,// + '?api-version=2.0',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': payload.length,
        'Authorization': 'Bearer ' + apikey
      }
    };


    if(debug)
    console.log(post_options);


    //setup request
    var post_req = http.request(post_options, function(res) {


      if(debug) console.log('STATUS: ' + res.statusCode);
      if(debug) console.log('HEADERS: ' + JSON.stringify(res.headers));


      if (res.statusCode == 404 || res.statusCode == 400) {
        console.log('err');
        return null;
      }


      if (res.statusCode == 200) {

        var data = "";
        res.setEncoding('utf8');
        res.on('data', function(chunk) {
          if(chunk)
            data += chunk;
        });

        res.on('end', function() {
            
            if(debug) console.log( JSON.parse(data) );      
              
            
          callback(JSON.parse(data));
        });

      } else {
        return 0;
      }

    }); 

    post_req.on('error', function(e) {
      console.log('error: ' + e.message);
    });


    //post data
    if (payload) {
      post_req.write(payload);
      post_req.end();
    }

  } //post : function(score) {



  ,start: function(apikey, workspace, services, job, payload, jobid, callback) {

    //stringify to get length
    this.payload = '';
    var httpg = require('https');

    //setup options
    var post_options = {
      host: 'ussouthcentral.services.azureml.net',
      port: '443',
      path: '/workspaces/' + workspace + '/services/' + services + '/' + job + '/' + jobid + '/start?api-version=2.0',
      method: 'POST',
      headers: {
        'Authorization': 'Bearer ' + apikey
      }
    };

    if(debug) console.log(post_options);

    //setup request
    var post_req = httpg.request(post_options, function(res) {

      if(debug)console.log('STATUS: ' + res.statusCode);
      if(debug)console.log('HEADERS: ' + JSON.stringify(res.headers));

      if (res.statusCode == 404) {
        console.log('err 404 start');
        return null;
      }

      if (res.statusCode == 200) {

        var data = "";
        res.setEncoding('utf8');
        res.on('data', function(chunk) {
          
          if(chunk)
          data += chunk;
          
        });

        res.on('end', function() {
          callback(jobid);
        });

      } else {
        
        return 0;
      }

    }); 

    post_req.on('error', function(e) {
      console.log('error start: ' + e.message);
    });

  } 
  
  ,get: function(apikey, workspace, services, job, payload, jobid, callback) {

    var httpg = require('https');

    //setup options
    var get_options = {
      host: 'ussouthcentral.services.azureml.net',
      port: '443',
      path: '/workspaces/' + workspace + '/services/' + services + '/' + job + '/' + jobid + '?api-version=2.0',
      method: 'GET',
      headers: {
        'Content-Length': 0,
        'Authorization': 'Bearer ' + apikey
      }
    };

    if(debug)
    console.log(get_options);  

    httpg.get(get_options, function(res) {

      if(debug)console.log('STATUS: ' + res.statusCode);
      if(debug)console.log('HEADERS: ' + JSON.stringify(res.headers));

      if (res.statusCode == 404) {
        console.log('err');
        return null;
      }

      if (res.statusCode == 200) {
        var data = "";
        res.setEncoding('utf8');
        res.on('data', function(chunk) {
          if(chunk)
          data += chunk;
        });

        res.on('end', function() {
          callback(JSON.parse(data));
        });

      }

      res.on('error', function(e) {
        console.log('error: ' + e.message);
      });


    }).on('error', function(e) {
      console.log('get error', e);
    }); 
    
  } 
  
  ,batch: function(url, callback) {

    var httpb = require('https');

    httpb.get(url, function(res) {
      if (res.statusCode == 404) {
        console.log('err');
        return null;
      }

      if (res.statusCode == 200) {
        var data = "";
        res.setEncoding('utf8');
        res.on('data', function(chunk) {
          if(chunk)
          data += chunk;
        });

        res.on('end', function() {
          callback(data);
        });

      }

      res.on('error', function(e) {
        console.log('error: ' + e.message);
      });


    }).on('error', function(e) {
      console.log('error batch', e);
    });

  }

  ,delete: function(apikey, workspace, services, job, payload, jobid, callback) {

    var httpd = require('https');

    //setup options
    var get_options = {
      host: 'ussouthcentral.services.azureml.net',
      port: '443',
      path: '/workspaces/' + workspace + '/services/' + services + '/' + job + '/' + jobid,
      method: 'DELETE',
      headers: {
        'Content-Length': 0,
        'Authorization': 'Bearer ' + apikey
      }
    };

    httpd.get(get_options, function(res) {

      if (res.statusCode == 404) {
        console.log('err');
        return null;
      }

      if (res.statusCode == 204) {
        callback('delete');
      }

      if (res.statusCode == 200) {
        var data = "";
        res.setEncoding('utf8');
        res.on('data', function(chunk) {
          if(chunk)
          data += chunk;
        });

        res.on('end', function() {
          callback(JSON.parse(data));
        });

      }

      res.on('error', function(e) {
        console.log('error: ' + e.message);
      });


    }).on('error', function(e) {
      console.log('error delete:', e, jobid);
    }); 

  } 

   ,trained: function(apikey, workspace, services, endpoint, payload, callback) {

    var https_trained = require('https'); 
    this.payload = JSON.stringify(payload); 

    if(debug) console.log(payload); 

    //setup options
    var post_options = {
      host: 'management.azureml.net',
      port: '443',
      path: '/workspaces/' + workspace + '/webservices/' + services + '/endpoints/' + endpoint,
      method: 'PATCH',
      headers: {
        'Content-Type':     'application/json;charset=utf-8',
        'Accept':           'application/json',
        'Content-Length':   this.payload.length,
        'Authorization':    'Bearer ' + apikey
      }
    };


    if(debug)
    console.log(post_options);


    //setup request
    var post_req = https_trained.request(post_options, function(res) {


      if(debug) console.log('STATUS: ' + res.statusCode);
      if(debug) console.log('HEADERS: ' + JSON.stringify(res.headers));


      if (res.statusCode == 404 || res.statusCode == 402 || res.statusCode == 401) {
        console.log('err');
        return null;
      }


      if (res.statusCode == 204) {
        callback('updated train model: '+endpoint);
      }


      if (res.statusCode == 200) {

        var data = "";
        res.setEncoding('utf8');
        res.on('data', function(chunk) {
          if(chunk)
            data += chunk;
        });

        res.on('end', function() {
          callback(JSON.parse(data));
        });

      } else {
        return 0;
      }

    }); 

    post_req.on('error', function(e) {
      console.log('error: ' + e.message);
    });


    //post data
    if (this.payload) {
      post_req.write(this.payload);
      post_req.end();
    }

  } 
  
}; 
