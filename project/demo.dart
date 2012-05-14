#import('pipeline.dart');
#import('dart:isolate');

main() {
  runPipeline(
    [new SpawnFunctionPipe("gen", generate),
     new SpawnFunctionPipe("3n+1", test3nPlus1),
     new SpawnFunctionPipe("out", printout)
     ]);
}

generate() => runSource((send) {
  for (var i = 1000000; i < 1000010; i++) {
    print("Sending $i");
    send(i);
  }
}); 

test3nPlus1() => runProcessor((message, send) {
  print("Checking if $message comes down to 4-2-1 in 1.000.000 cycles");

  var cycles = 0;
  for (var n = message; cycles < 1000000 && n != 1 && n != 2 && n != 4; cycles++) {
    if (n.toInt().isOdd()) {
      n = n*3 + 1;
    } else {
      n /= 2;
    }
  }
  
  send({ "cycles": cycles, "value": message });
});

increase() => runProcessor((message, send) {
  print("Increasing $message");
  send(message+1);
});

printout() => runSink((message) { 
  print("Printing $message"); 
});
