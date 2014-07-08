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

// Create schema for company info
var UserSchema = new mongoose.Schema({
    cmpId:{type:String},  // set constraints
    cmpName:{type:String},
    cmpAddress:{type:String}
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
        callback(null,null);
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
            callback(null,null);
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
        callback(null,null);
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
            callback(null,null);
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
                callback(null,result.length );
            }
        });
    });


}

// Company details

// Create schema for company info
var companySchema = new mongoose.Schema({
    cmpId:{type:String},  // set constraints
    cmpName:{type:String},
    cmpAddress:{type:String}
});

// Create a instance of collection
var companyCollection = sourceDB.model('companyInfo', companySchema);
var organizationCollection = destinationDB.model('organizationInfo', companySchema);


function findCompanyData(callback) {

    companyCollection.find({}).limit(0).exec(function(err, result) {

        if (err) {
            callback(err);
            console.log('Error while saving user...' + err);
            return;
        }
        console.log('Found Company Data:' + result);
        callback(null,null);
    });
}

function cleanCompanyDB(callback){

    // Remove record from data base
    companyCollection.remove({}, function(err, result) {
        if (err) {
            callback(err);
            console.log('Error while remove  data from DB..' + err);
            return;
        }
        else {
            console.log('Clean data from company collection successfully :' + result);
            callback(null,null);
        }
    });
}


function insertCompanyData(callback) {
    //Prepare user data object
    var companyData = [{
        cmpId:'1001',
        cmpName:'IntelliGrape',
        cmpAddress:'L 6 Noida NSEZ'
    },
        {
            cmpId:'1002',
            cmpName:'Facebook',
            cmpAddress:'Noida'
        },
        {
            cmpId:'1003',
            cmpName:'Google',
            cmpAddress:'Banglur'
        },{
            cmpId:'1004',
            cmpName:'Oracle',
            cmpAddress:'USA'
        },{
            cmpId:'1005',
            cmpName:'Imptus',
            cmpAddress:'Noida NSEZ'
        },{
            cmpId:'1006',
            cmpName:'Edulabs',
            cmpAddress:'Greater Noida'
        },{
            cmpId:'1007',
            cmpName:'HCL',
            cmpAddress:'Noida Sector 16'
        },{
            cmpId:'1008',
            cmpName:'Ericsion',
            cmpAddress:'Noida Sector 15'
        },{
            cmpId:'1009',
            cmpName:'IBM',
            cmpAddress:'Delhi'
        },{
            cmpId:'1010',
            cmpName:'TCS',
            cmpAddress:'Gurgoun'
        }
    ];
    var tasks = [];

    companyData.forEach(function(ud) {
        tasks.push(function(callback) {
            new companyCollection(ud).save(function (err) {
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
            console.log(result.length + " Company records insert successfully ");
            callback();
        }
    });
}

function findOrganizationDB(callback) {
    organizationCollection.find({}).exec(function(err, result) {

        if (err) {
            callback(err);
            console.log('Error while getting dummy data...' + err);
            return;
        }
        console.log('Found Organization data:' + result);
        callback(null.null);
    });
}

function cleanOrganizationDB(callback){

    // Remove record from data base
    organizationCollection.remove({}, function(err, result) {
        if (err) {
            callback(err);
            console.log('Error while remove data from DB..' + err);
            return;
        }
        else {
            console.log('Clean data from organization collection successfully :' + result);
            callback(null,null);
        }
    });
}

function CopyCompanyData(callback) {
    var totalRecord;
    var recordCount = 1;
    companyCollection.find({}).exec(function (err, u) {
        totalRecord = u.length;
        var tasks = [];
        u.forEach(function(ud){
            tasks.push( function(callback) {
                new organizationCollection(ud).save(function (err) {
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
                console.log(result.length + " Company records copy successfully ");
                callback(null,result.length );
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
            callback(null,null);
        }
    });
}

//Copy all source collections to destination collection


function createUserCopy(callback) {
    async.series([
        cleanUserDB,
        findUserData,
        insertUserData,
        findUserData,
        cleanPersonDB,
        CopyUserData
    ],function(err, result){
        if(err) {
            console.log('Getting some error....');
        }else {
            if(result) {
                var userRecord = result[5] + " User records";
                callback(null, userRecord);
            }
        }
    });
}

function createCompanyCopy(callback) {
    async.series([
        cleanCompanyDB,
        findCompanyData,
        insertCompanyData,
        findCompanyData,
        cleanOrganizationDB,
        CopyCompanyData
    ],function(err, result){
        if(err) {
            console.log('Getting some error....');
        }else {
            if(result) {
                var companyRecord = result[5] + " Company records";
                callback(null, companyRecord);
            }
        }
    });
}

async.parallel([createUserCopy, createCompanyCopy],function(err, result){
    if(err) {
        console.log('Getting some error....');
    }else {
        console.log('Successfully copy db with  ' + result);
    }
});