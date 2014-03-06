import inspect
import json
import re
import traceback

from bson import json_util

####  CONSTANTS

TYPE_FIELD = '_type'

###############################################################################
# Maker Exception class
###############################################################################
class MakerException(Exception):
    def __init__(self, message,cause=None):
        self.message  = message
        self.cause = cause

    def __str__(self):
        return self.message

###############################################################################
def resolve_class(kls):
    if kls == "dict":
        return dict

    try:
        parts = kls.split('.')
        module = ".".join(parts[:-1])
        m = __import__( module )
        for comp in parts[1:]:
            m = getattr(m, comp)
        return m
    except Exception, e:
        raise MakerException("Cannot resolve class '%s'. Cause: %s" % (kls, e))

###############################################################################

class Maker(object):
    ###########################################################################
    def __init__(self,
                 type_bindings=None,
                 default_type=None,
                 object_descriptor_resolver=None):

        self.type_bindings = type_bindings
        self.default_type = default_type or dict
        self.object_descriptor_resolver = object_descriptor_resolver

        self.ignore_fields = []

    ###########################################################################
    def make(self, datum):

        # sequences
        sequence_attrs = {'__getitem__', '__contains__', '__iter__',
                          '__reversed__', 'index', 'count'}
        if sequence_attrs.issubset(set(dir(datum))):
            return self._make_list(datum)

        # mappings
        mapping_attrs = {'__iter__', '__getitem__', '__contains__', 'keys',
                         'items', 'values', 'get', '__eq__', '__ne__'}
        if mapping_attrs.issubset(set(dir(datum))):
            return self._make_object(datum)

        return datum


    ###########################################################################
    def _make_list(self, datum):
        return map(lambda elem: self.make(elem), datum)

    ###########################################################################
    def _make_object(self, datum):

        desc_resolver = self.object_descriptor_resolver
        if desc_resolver and desc_resolver.is_datum_descriptor(datum):
            datum = desc_resolver.resolve_datum_descriptor(datum)

        # instantiate object
        result = self.instantiate(datum)

        # define properties
        for key, value in datum.items():
            # Include _type field for dict types only
            if (key not in self.ignore_fields and
                (key != TYPE_FIELD or isinstance(result, dict))):
                prop_name = self.resolve_property_name(result, key)
                prop_val = self.make(value)

                self._set_object_property(result, prop_name, prop_val)

        return result

    ###########################################################################
    def instantiate(self, datum):

        obj_type = dict

        if TYPE_FIELD in datum :
            type_name = datum[TYPE_FIELD]
            if isinstance(type_name, (str, unicode)):
                obj_type = self.resolve_type(type_name)
                if not obj_type:
                    raise MakerException("Could not resolve _type %s" %
                                         type_name)
            elif inspect.isclass(type_name):
                obj_type = type_name
            else:
                raise MakerException("Invalid _type value '%s'. _type can "
                                "be a string or a class" % type_name)

        return obj_type()

    ###########################################################################
    def _set_object_property(self, obj, prop, value):
        try:
            if isinstance(obj, dict):
                obj[prop] = value
            else:
                setattr(obj, prop, value)
        except Exception, e:
            msg = ("Error while trying to set property %s for object %s."
                   " Cause: %s, Trace: %s" % (prop, obj, e,
                                              traceback.format_exc()))
            raise Exception(msg)

    ###########################################################################
    def resolve_type(self, type_name):
        if self.type_bindings and type_name in self.type_bindings:
            return resolve_class(self.type_bindings[type_name])
        else:
            return resolve_class(type_name)

    ###########################################################################
    def resolve_property_name(self, obj, datum_prop_name):
        """
        Resolves the property name of the specified obj. If the obj has an
        un-cameled property of the specified property ==> returns un-cameled
        ELSE: return property as is
        """
        # TODO use a mapping
        try:
            datum_prop_name = datum_prop_name.encode('ascii', 'ignore')
            uncameled_prop = un_camelcase(datum_prop_name)
            if (hasattr(obj, uncameled_prop) and
                    not hasattr(obj, datum_prop_name)):
                return uncameled_prop
            else:
                return datum_prop_name
        except Exception, e:
            msg = ("Error while trying to resolve property name '%s' for "
                   "object %s."
                   " Cause: %s, Trace: %s" % (datum_prop_name, obj, e,
                                              traceback.format_exc()))
            raise Exception(msg)

###############################################################################
def un_camelcase(property_name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', property_name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

###############################################################################
# GenericDocObject
###############################################################################
class GenericDocObject(object):

    ###########################################################################
    def __init__(self, document=None):
        document = document or {}
        self.__dict__['_document'] = document

    ###########################################################################
    @property
    def document(self):
        return self.__dict__['_document']

    ###########################################################################
    def __getattr__(self, name):
        if self._document.has_key(name):
            return self._document[name]

    ###########################################################################
    def has_key(self, name):
        return self._document.has_key(name)

    ###########################################################################
    def __setattr__(self, name, value):
        if hasattr(getattr(self.__class__, name, None), '__set__'):
            # THANK YOU http://bit.ly/HOTMsT
            return object.__setattr__(self, name, value)
        self._document[name] = value

    ###########################################################################
    def __delattr__(self, name):
        if hasattr(getattr(self.__class__, name, None), '__delete__'):
            # THANKS AGAIN http://bit.ly/HOTMsT
            return object.__delattr__(self, name)
        del self._document[name]

    ###########################################################################
    def __getitem__(self, name):
        return self.__getattr__(name)

    ###########################################################################
    def __setitem__(self, name, value):
        return self.__setattr__(name, value)

    ###########################################################################
    def __delitem__(self, name):
        return self.__delattr__(name)

    ###########################################################################
    def __contains__(self, name):
        return self.has_key(name)

    ###########################################################################
    def __str__(self):
        return document_pretty_string(self._document)

    ###########################################################################
    def __repr__(self):
        return self._document.__repr__()

    ###########################################################################
    def items(self):
       return  dict((key, self[key]) for key in self._document.keys()).items()

###############################################################################
# other helpful utilities... okay, just document_pretty_string()
###############################################################################
def document_pretty_string(document):
    return json.dumps(document, indent=4, default=json_util.default)

###############################################################################
MAKER = Maker()

def o(datum):
    return MAKER.make(datum)
