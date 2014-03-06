__author__ = 'abdul'

import logging

from maker import Maker
from pymongo.errors import AutoReconnect

from robustify.robustify import robustify
from bson import DBRef

###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

###############################################################################
# HELPERS
###############################################################################

def _raise_if_not_autoreconnect(exception):
    if not isinstance(exception, AutoReconnect):
        logger.debug("Reraising non-AutoReconnect exception: %s" % exception)
        raise
    logger.warn("Caught AutoReconnect exception!")

###############################################################################
def _raise_on_failure():
    raise


###############################################################################
# ObjectCollection
###############################################################################
class ObjectCollection(object):

    ###########################################################################
    def __init__(self, collection, clazz=None, type_bindings=None):
        self.collection = collection
        self.maker =  Maker(type_bindings=type_bindings,
            default_type=clazz,
            object_descriptor_resolver=self)
        self.database = collection.database
        self.name = collection.name

    ###########################################################################
    # Object descriptor Resolver implementations
    ###########################################################################


    ###########################################################################
    def is_datum_descriptor(self, value):
        return type(value) is DBRef

    ###########################################################################
    def resolve_datum_descriptor(self, desc):
        db_ref = desc
        ref_collection_name = db_ref.collection
        ref_collection = self.database[ref_collection_name]
        return ref_collection.find_one({"_id": db_ref.id})

    ###########################################################################
    # queries
    ###########################################################################
    @robustify(max_attempts=5, retry_interval=2,
               do_on_exception=_raise_if_not_autoreconnect,
               do_on_failure=_raise_on_failure)
    def find(self, query=None, sort=None, limit=None):
        if query is None or (query.__class__ == dict):
            result = self.find_iter( query, sort, limit )
            # TODO: this is bad for large result sets potentially
            return [ d for d in result ]
        else:
            return self.find_one({ "_id" : query }, sort=sort)

    ###########################################################################
    def find_iter(self, query=None, sort=None, limit=None):
        if query is None or (query.__class__ == dict):
            if limit is None:
                documents = self.collection.find(query, sort=sort)
            else:
                documents = self.collection.find(query, sort=sort, limit=limit)

            for doc in documents:
                yield self.make_obj( doc )
        else:
            # assume query is _id and do _id lookup
            yield self.find_one({ "_id" : query }, sort=sort)

    ###########################################################################
    @robustify(max_attempts=5, retry_interval=2,
               do_on_exception=_raise_if_not_autoreconnect,
               do_on_failure=_raise_on_failure)
    def find_one(self, query=None, sort=None):
        result = self.collection.find_one(query, sort=sort)
        return self.make_obj( result )

    ###########################################################################
    @robustify(max_attempts=5, retry_interval=2,
               do_on_exception=_raise_if_not_autoreconnect,
               do_on_failure=_raise_on_failure)
    def find_and_modify(self, query=None, update=None, sort=None, **kwargs):
        result = self.collection.find_and_modify(query=query, update=update,
                                                 sort=sort, **kwargs)
        return self.make_obj(result)

    ###########################################################################
    @robustify(max_attempts=5, retry_interval=2,
               do_on_exception=_raise_if_not_autoreconnect,
               do_on_failure=_raise_on_failure)
    def update(self, spec, document, upsert=False, manipulate=False,
                     safe=None, multi=False, check_keys=True, **kwargs):
        self.collection.update(spec=spec, document=document, upsert=upsert,
                               manipulate=manipulate, safe=safe, multi=multi,
                               check_keys=check_keys, **kwargs)

    ###########################################################################
    @robustify(max_attempts=5, retry_interval=2,
               do_on_exception=_raise_if_not_autoreconnect,
               do_on_failure=_raise_on_failure)
    def save_document(self, document):
        self.collection.save(document)

    ###########################################################################
    def make_obj( self, doc ):
        return self.maker.make(doc)

    ###########################################################################
    def insert(self, object):
        pass

    ###########################################################################
    def save(self, object):
        pass

    ###########################################################################
    def remove(self, object):
        pass

    ###########################################################################
    @robustify(max_attempts=5, retry_interval=2,
               do_on_exception=_raise_if_not_autoreconnect,
               do_on_failure=_raise_on_failure)
    def remove_by_id(self, id):
        self.collection.remove({"_id": id})

