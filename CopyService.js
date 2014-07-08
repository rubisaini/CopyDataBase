//Include mongoose into project
var mongoose = require('mongoose');
var async = require('async');

//Create a data base
var URLString ='mongodb://localhost/OrganizationSource';
var DestinationString ='mongodb://localhost/OrganizationDestination';

// trace the status of program
console.log('Node JS server started....');

//Connect to source data base
var sourceDB = mongoose.createConnection(URLString);
var destinationDB = mongoose.createConnection(DestinationString);

// Create schema for user info
var UserSchema = new mongoose.Schema({
    id:{type:String},  // set constraints
    name:{type:String},
    designation:{type:String},
    bloodGroup:{type:String},
    address:{type:String},
    age:{type:Number, min:18, max:65}
});


// Create a instance of collection
var userCollection = sourceDB.model('userInfo', UserSchema);
var personCollection = destinationDB.model('personInfo', UserSchema);


function findUserData(callback) {

    userCollection.find({}).sort({age:1}).limit(0).exec(function(err, result) {

        if (err) {
            callback(err);
            console.log('Error while saving user...' + err);
            return;
        }
        console.log('Found User Data:' + result);
        callback();
    });
}

function cleanUserDB(callback){

    // Remove record from data base
    userCollection.remove({}, function(err, result) {
        if (err) {
            callback(err);
            console.log('Error while remove  data from DB..' + err);
            return;
        }
        else {
            console.log('Clean data from user collection successfully :' + result);
            callback();
        }
    });
}


function insertUserData(callback) {
    //Prepare user data object
    var userData = [{
        id:'IG001',
        name:'User 1',
        designation:'Developer',
        bloodGroup:'B+',
        address:'Noida sec-41',
        age:25
    },
        {
            id:'IG002',
            name:'User 2',
            designation:'Developer',
            bloodGroup:'B+',
            address:'Noida sec-41',
            age:25
        },{
            id:'IG0003',
            name:'User 3',
            designation:'Developer',
            bloodGroup:'B+',
            address:'Noida sec-41',
            age:25
        },
        {
            id:'IG0004',
            name:'User 4',
            designation:'Developer',
            bloodGroup:'B+',
            address:'Noida sec-41',
            age:25
        },
        {
            id:'IG0005',
            name:'User 5',
            designation:'Developer',
            bloodGroup:'B+',
            address:'Noida sec-41',
            age:25
        }
        ,{
            id:'IG0006',
            name:'User 6',
            designation:'Developer',
            bloodGroup:'B+',
            address:'Noida sec-41',
            age:25
        },
        {
            id:'IG0007',
            name:'User 7',
            designation:'Developer',
            bloodGroup:'B+',
            address:'Noida sec-41',
            age:25
        },
        {
            id:'IG0008',
            name:'User 8',
            designation:'Developer',
            bloodGroup:'B+',
            address:'Noida sec-41',
            age:25
        }
        ,
        {
            id:'IG0009',
            name:'User 9',
            designation:'Developer',
            bloodGroup:'B+',
            address:'Noida sec-41',
            age:25
        }
        ,
        {
            id:'IG00010',
            name:'User 10',
            designation:'Developer',
            bloodGroup:'B+',
            address:'Noida sec-41',
            age:25
        }
    ];

    var tasks = [];
    var totalRecord = userData.length;
    var recordCount = 1;
    userData.forEach(function(ud) {
        tasks.push(function(callback) {
            new userCollection(ud).save(function (err) {
                if (err) {
                    console.log("there is some error while copying database: " + err);
                    callback(err);
                    return;
                } else {
                    callback();
                }
            });
        });
    });

    async.series(tasks, function(err, result){
        if(err) {
            console.log('Error to copy data');
        }else {
            console.log(result.length + " User records insert successfully ");
            callback();
        }
    });

}

function findPersonDB(callback) {
    personCollection.find({}).exec(function(err, result) {

        if (err) {
            callback(err);
            console.log('Error while getting dummy data...' + err);
            return;
        }
        console.log('Found Person data:' + result);
        callback();
    });
}

function cleanPersonDB(callback){

    // Remove record from data base
    personCollection.remove({}, function(err, result) {
        if (err) {
            callback(err);
            console.log('Error while remove data from DB..' + err);
            return;
        }
        else {
            console.log('Clean data from person collection successfully :' + result);
            callback();
        }
    });
}

function CopyUserData(callback) {
    var totalRecord;
    var recordCount = 1;
    userCollection.find({}).exec(function (err, u) {
        totalRecord = u.length;
        var tasks = [];
        u.forEach(function(ud){
           tasks.push( function(callback) {
               new personCollection(ud).save(function (err) {
                   if (err) {
                       console.log("there is some error while copying database: " + err);
                       callback(err);
                       return;
                   } else {
                       callback();
                   }
               });
           });
        });

        async.series(tasks, function(err, result){
            if(err) {
                console.log('Error to copy data');
            }else {
                console.log(result.length + " User records copy successfully ");
                callback();
            }
        });
    });


}

function closeConncetion(callback) {
    mongoose.disconnect(function(err){
        if (err) {
            callback(err);
            console.log('Error while disconnect DB..' + err);
            return;
        }
        else {
            console.log('Close connection');
            callback();
        }
    });
}

//Copy all source collections to destination collection
async.series([
    cleanUserDB,
    findUserData,
    insertUserData,
    findUserData,
    cleanPersonDB,
    CopyUserData,
    findPersonDB
],function(err){
    if(err) {
        console.log('Getting some error....');
    }else {
        console.log('All operations has done successfully');
    }
});

