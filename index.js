require('dotenv').config();
const AWS = require('aws-sdk');
const mysql = require('mysql');

AWS.config.update({
	accessKeyId: "AKIAU26OYWGVDDPQBUJC",
	secretAccessKey: "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"
});

const csv = require('csvtojson');
const S3 = new AWS.S3();
const params = {
	Bucket: 'csv-to-db-bucket-for-lambda',
	Key: 'employees.csv'
};

exports.handler = async (event) => {

	let con = await mysql.createConnection({
		host: "csv-dumb.cahbe1rahqg6.ap-south-1.rds.amazonaws.com",
		user: "admin",
		password: "password"
	});
	con.connect(function (err) {
		if (err) throw err;
		console.log("DB Connected!");
	});

	let res = null;
	const stream = S3.getObject(params).createReadStream();
	const json = await csv().fromStream(stream);
	console.log('json: ', json);

	let sql = `insert into employees.emp values `
	json.forEach((e, index) => {
		sql += `(${e.id}, \'${e.name}\')`;
		if (index !== json.length - 1) sql += `, `
		else sql += `; `
	})
	console.info('sql: ', sql);

	con.query(sql, function (err, result) {
		if (err) throw err;
		console.log("Query Result: " + JSON.stringify(result));
		res = JSON.stringify(result);
	});
	return res;
}