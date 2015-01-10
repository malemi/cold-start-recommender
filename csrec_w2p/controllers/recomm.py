# -*- coding: utf-8 -*-
# try something like

import gluon.contrib.simplejson

def insertrating(): #post
    user = request.vars['user']
    item = request.vars['item']
    rating = request.vars['rating']
    engine.insert_rating(user, item, rating)
    return gluon.contrib.simplejson.dumps({})

def insertitem(): #post
    item = request.vars
    engine.insert_item(item, _id='id')
    return gluon.contrib.simplejson.dumps(item)

def recommend(): # get
    user = request.vars['user']
    if request.vars['max_recs']:
        max_recs = int(request.vars['max_recs'])
    else:
        max_recs = 10

    try:
        fast_param = request.vars['fast']
    except:
        fast_param = None

    if fast_param:
        fast = True
    else:
        fast = False

    retVals =  engine.get_recommendations(user, max_recs=max_recs, fast=fast)
    return gluon.contrib.simplejson.dumps(retVals)

def reconcile(): #post
    try:
        old = request.vars['old']
        new = request.vars['new']
    except:
        raise HTTP(400)
    engine.reconcile(old, new)
    return gluon.contrib.simplejson.dumps({})

def info(): #get
    user = request.vars['user']
    retVals = engine.get_user_info(user)
    return gluon.contrib.simplejson.dumps(retVals)

def items(): #get
    n = request.vars['n']
    try:
        retVals = engine.get_items(n)
    except:
        raise HTTP(400)
    return gluon.contrib.simplejson.dumps(retVals)
