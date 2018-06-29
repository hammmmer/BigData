var express = require('express');
var app = express();
app.use('/public', express.static('public'));

app.all('/', function(req, res, next) {
    res.header("Access-Control-Allow-Origin", "*");
    res.header('Access-Control-Allow-Methods', 'PUT, GET, POST, DELETE, OPTIONS');
    res.header("Access-Control-Allow-Headers", "X-Requested-With");
    res.header('Access-Control-Allow-Headers', 'Content-Type');
    next();
}); 
//引入mysql模块  
var mysql=require('mysql');  
//创建连接  
var conn=mysql.createConnection({  
    host:'127.0.0.1',  
    user:'root',  
    password:'123456',  
    database:'bigdata'  
});  
//进行连接  
conn.connect();
app.get('/', function (req, res) {
    //以insert写sql语句  
    var sql="select * from kwcloud order by num desc limit 100;"  
    conn.query(sql,function(err,result){  
     if(err){   
         console.log(err)   
      }else{
        var resjson = []
        for(var i = 0; i < result.length; i ++) {
            resjson.push({
                name: result[i].kw,
                value: result[i].num
            });
        }
        res.json(resjson);
     }  
    });
});

var server = app.listen(3000, function () {
  var host = server.address().address;
  var port = server.address().port;

  console.log('Example app listening at http://%s:%s', host, port);
});