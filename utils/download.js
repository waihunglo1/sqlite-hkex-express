const { PythonShell } = require('python-shell');

let options = {
  mode: 'text',
  pythonOptions: ['-u'], // get output in real-time
  scriptPath: 'scripts', // path to your python scripts
  args: [] // Arguments passed to the Python script
};

PythonShell.run('dn-data.py', options, function (err, results) {
  if (err) throw err;
  // results is an array of messages collected during execution
  console.log('results: %j', results);
});