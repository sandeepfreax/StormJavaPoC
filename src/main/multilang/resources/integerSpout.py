import storm


class integerSpout(storm.Spout):

    def initialize(self, conf, context):
        self._conf = conf
        self._context = context
        self._i = 0


    def nextTuple(self):

        while self._i<100:
            storm.emit([(self._i),(self._i/10)])
            self._i+=1


integerSpout().run()
