import storm


class plusTenBolt(storm.BasicBolt):

    def initialize(self, conf, context):
        self._conf = conf
        self._context = context


    def process(self, tup):
        number = tup.values[0]
        storm.emit([number+10])


plusTenBolt().run()
