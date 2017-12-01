###########
# imports #
###########


from json import load
import time
import os.path
import logging
from flask import Flask, jsonify, abort, make_response, render_template
import pandas as pd
from sqlalchemy import create_engine, exc
from sqlalchemy.engine.url import URL


#########
# config #
##########


app = Flask(__name__)
fname = 'dataframe.json'


#############
# functions #
#############


def db_connect():
    """
    return db engine instance
    """
    DB = {
        'drivername': 'mysql',
        'host': 'cha-nerd-01.nix.csmodule.com',
        'port': '3306',
        'username': 'nerd_ext',
        'password': 'Netent4ever!',
        'database': 'nerd'
    }
    return create_engine(URL(**DB))

def db_status():
    """
    test db engine connectivity
    """
    try:
        engine = db_connect()
        engine.connect()
        return True
    except exc.OperationalError:
        return False


def enabled_routes():
    """automatically generated list of routes"""
    func_list = {}
    for rule in app.url_map.iter_rules():
        if rule.endpoint != 'static':
            func_list[rule.rule] = eval(rule.endpoint).__doc__
    return {'routes': func_list}


def generate_persistent_storage():
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        filemode='a',
        filename='df.log',
        level=logging.INFO
    )

    # HANDLER = logging.handlers.RotatingFileHandler('app.log', maxBytes=1000, backupCount=3)
    # FORMATTER = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    # HANDLER.setFormatter(FORMATTER)
    # LOGGER.addHandler(HANDLER)
    logger = logging.getLogger()


    fname = 'dataframe.json'
    cutoff = time.time() - 1 * 10
    regen = False


    try:
        with open(fname, 'r'):
            if os.path.getmtime(fname) < cutoff:
                print "stale cache"
                logger.info("stale cache")
                regen = True
            else:
                print "cache hit!"
                logger.info("cache hit!")
    except IOError:
        print("error opening %s, regenerating cache...") % fname
        logger.warn('%s not found: regenerating...', fname)
        regen = True

    if regen:
        try:
            engine = db_connect()
            df = pd.read_sql_query('SELECT * FROM nerd;', con=engine)
            logger.info('running query')

            df['jurisdiction'] = df['jurisdiction'].str.replace('malta, uk', 'malta-uk')
            df['jurisdiction'] = df['jurisdiction'].str.replace('alderney, uk', 'alderney-uk')
            df['jurisdiction'] = df['jurisdiction'].str.replace('costa rica', 'costarica')

            df.to_json('dataframe.json', orient='records')
        except exc.OperationalError as e:
            print e
            logger.error(e)


@app.errorhandler(404)
def not_found(error):
    """404 handler"""
    return make_response(jsonify({'error': '404 - check url'}), 404)


##########
# routes #
##########


@app.route('/')
def status():
    """status page"""
    datasource = {'data source':{'connection': db_status()}}
    routes = enabled_routes()
    try:
        storage = {'storage':{'filename': fname, 'created': time.ctime(os.path.getctime(fname)), 'modified': time.ctime(os.path.getmtime(fname))}}
    except OSError as e:
        storage = {'storage': str(e)}
    return jsonify(datasource, storage, routes)


@app.route('/dataset')
def data_to_html():
    """view loaded dataset (html)"""
    try:
        data = pd.read_json(fname)
    except ValueError:
        return jsonify({'error': 'data source inaccessible'})
    columns = ['casinoid', 'status', 'site', 'jurisdiction', 'mobile', 'seamlesswallet', 'prodnodes', 'prodver', 'prodgee', 'proddbserver', 'proddbname', 'testnodes', 'testver', 'testgee', 'testdbserver', 'testdbname']
    dataframe = data.filter(items=columns)
    return render_template("dataframe.html", data=dataframe.to_html(index=True).replace('<table border="1" class="dataframe">', '<table class="table table-hover" style="font-size:13px;">'))


@app.route('/api/casinoid/<casinoid>/status')
def get_status(casinoid):
    """return casinoid status"""
    try:
        with open(fname, 'r') as data_file:
            json = load(data_file)
        for casino in json:
            if casino["casinoid"] == casinoid:
                cstatus = casino["status"]
                break
        else:
            cstatus = "undefined"
        return jsonify({'status': cstatus})
    except IOError as e:
        return jsonify({"error": str(e)})


@app.route('/api/casinoid/<casino_id>')
def casino_json(casino_id):
    """return json for casinoid"""
    try:
        with open(fname, 'r') as data_file:
            data = load(data_file)
        output_dict = [x for x in data if x['casinoid'] == casino_id]
        if not output_dict:
            abort(404)
        return jsonify(output_dict)
    except IOError:
        return jsonify({'error': 'couldn\'t load JSON data'})


@app.route('/updatedb')
def update():
    generate_persistent_storage()
    return "I wrote som json, probably..."


@app.route('/api/jurisdiction/<jurisdiction>')
def jurisd_json(jurisdiction):
    """return json for jurisdiction"""
    try:
        with open(fname) as data_file:
            data = load(data_file)
        output_dict = [x for x in data if x['jurisdiction'] == jurisdiction]
        if not output_dict:
            abort(404)
        return jsonify(output_dict)
    except IOError:
        return jsonify({"error": "couldn't load JSON data"})
