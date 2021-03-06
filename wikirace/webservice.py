import logging
from time import sleep

from flask import Flask
from flask_restful import reqparse, Api, Resource

from wikirace.configuration import OUTPUT_COLNM, DMN, INGESTION_TOPIC
from wikirace.run import make_tmplt
from wikirace.utils.kfkpywrapper import KfkProducer
from wikirace.utils.mongo import mongo_connect
from wikirace.utils.string import hash_str

app = Flask(__name__)
api = Api(app)

# set up request parser
parser = reqparse.RequestParser()
prod = KfkProducer(INGESTION_TOPIC)


class WebServiceInfo(Resource):
    def get(self):
        return 'Webservice Version 1.0'


class WikiService(Resource):
    def query_wikidb(self, fr, to):
        """ query db for result / wait if it does not exists

        :param fr: from
        :param to: to
        :return:
        """
        _id = hash_str('{}||{}'.format(fr, to))
        q = {'_id': _id}

        while True:
            res = dbcon_oput.find_one(q)

            if res:
                return res['ans']

            logging.info('Please wait..')
            sleep(2)

    def get(self):

        args = parser.parse_args()
        parser.add_argument('fr', default=None, type=str)
        parser.add_argument('to', default=None, type=str)

        if not args.fr or not args.to:
            return 'Please pass both fr and to. example: ' \
                   'curl IP:5000/api/v1/wiki?fr=MODEL2&to=test'

        fr = '{}/{}/{}'.format(DMN, 'wiki', args.fr)
        to = '{}/{}/{}'.format(DMN, 'wiki', args.to)

        # submit the request
        entry = make_tmplt(url=fr, dst=to, title=fr)
        prod.produce(entry)

        # query result collection
        return self.query_wikidb(fr, to)

# route resource here
api_base_url = '/api/v1'
api.add_resource(WebServiceInfo, '/')
api.add_resource(WikiService, api_base_url + '/wiki')


# ######### EXAMPLES: #################
# curl 127.0.0.1:5000/api/v1/wiki\?model=MODEL2


if __name__ == '__main__':
    dbcon_oput = mongo_connect(col_nm=OUTPUT_COLNM)

    logging.info('Successfully loaded the app')
    app.run(host='0.0.0.0', debug=True, threaded=True)
