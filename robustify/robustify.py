import sys
import types
import inspect

from time import sleep, time as now

################################################################################
# Dealing with explosions - the Context Manager way
################################################################################

class robust_execution_context(object):

    def __init__(self, handle_it=None, abort_phrase="aborting", logger=None):
        self._handle_it = (lambda: 42) if handle_it is None else handle_it
        self._logger = _logger_for_sure(logger)
        self._abort_phrase = abort_phrase
        self._abort_banner = _banner("WHOOPS!", abort_phrase, "& rolling back!")

    def __enter__(self):
        def handle_it_n_rollback():
            self._handle_it()
            self.rollback()
            #set things up
        self._rollback_hook = Hook(lifo=True)
        self._cleanup_hook = Hook()
        (self._begin_to, self._abort) = make_abort_handler(handle_it_n_rollback,
            self._abort_banner,
            self._logger,
            swallow=True)
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        #tear things down
        if exc_type is None:
            self.cleanup()
        else:
            self.abort(exc_type, exc_value, exc_traceback, swallow=True)
            return False        # i.e., will propagate error/exception

    def push_rollback_step(self, f):
        self._rollback_hook.add(f)

    def clear_rollback_steps(self):
        self._rollback_hook.clear()

    def push_cleanup_step(self, f):
        self._cleanup_hook.add(f)

    def push_unwind_step(self, f):
        """Pushes f so as to execute during either normal OR abortive exit."""
        self.push_rollback_step(f)
        self.push_cleanup_step(f)

    def begin_to(self, phase, morespew=""):
        self._begin_to(phase, morespew)

    def abort(self, exc_type, exc_value, exc_traceback, swallow=False):
        self._abort(exc_value, swallow=swallow)

    def rollback(self):
        self._rollback_hook.run()
        self.clear_rollback_steps()

    def cleanup(self):
        self._cleanup_hook.run()

def _banner(*msg_parts):
    banner_bracket = " *** !!! *** !!! *** !!! *** "
    return banner_bracket + " ".join(msg_parts) + banner_bracket

################################################################################
# Dealing with explosions - the Functional way
################################################################################

def reraise(new_exc):
    (et, ev, etb) = sys.exc_info()
    raise new_exc, None, etb

def make_abort_handler(handle_it, abort_phrase="aborting", logger=None,
                       swallow=False):
    _phase = { 'ima' : "get it done" } # so you call this a closure...
    logger = _logger_for_sure(logger)
    def begin_to(phase, morespew=""):
        _phase['ima'] = phase
        logger.info("Setting about to %s %s..." % (phase, morespew))
    def abort(err=None, swallow=swallow):
        (et, ev, etb) = sys.exc_info()
        whats_happening = ("%s after failing to %s" %
                           (abort_phrase, _phase['ima']))
        logger.error("%s : %s" % (whats_happening, err or "Ow."))
        logger.info(_banner(" handler execution commencing "))
        try:
            handle_it()
        except Exception, e:
            logger.error("Exception while %s : %s" % (whats_happening, e))
            reraise(e)
        except:
            logger.error("Unspecified error while %s - gosh!" % whats_happening)
            raise
        finally:
            logger.info(_banner(" handler execution concluded "))

        # Successfully handled original issue; now resume aborting
        if err is None:
            if not swallow:
                raise
        else:
            if not swallow:
                raise err, None, etb
    return (begin_to, abort)


class Hook(object):
    """
    A Hook is a place to hang sequences of functions you'd like to run
    potentially at some future time.
    Order is guaranteed; `lifo` arg to constructor says it's reversed.
    """
    def __init__(self, lifo=False):
        self.hook_functions = []
        self.lifo = lifo

    def clear(self):
        self.hook_functions = []

    def add(self, hook_fcn):
        if hook_fcn:
            self.hook_functions.append(hook_fcn)

    def run(self):
        stuff_to_do = (self.hook_functions[::-1] if self.lifo else
                       self.hook_functions)
        return [hfcn() for hfcn in stuff_to_do]


def _logger_for_sure(a_logger_maybe):
    if a_logger_maybe is not None:
        almbrs = dict(inspect.getmembers(a_logger_maybe))
        if all(map(lambda n: almbrs.has_key(n) and inspect.isroutine(almbrs[n]),
            ['error', 'warn', 'info', 'debug'])):
            return a_logger_maybe
    return NoOpLogger()

class NoOpLogger(object):
    def error(*_, **__):
        pass
    def warn(*_, **__):
        pass
    def info(*_, **__):
        pass
    def debug(*_, **__):
        pass

################################################################################
# Living with properties (are they attributes? items? "pythonic"?)
################################################################################

def getprop(x, prop_name, val_if_missing=None):
    """
    Returns the given property of its first argument.
    """
    x_has_prop = x and hasattr(x, "has_key") and x.has_key(prop_name)
    return x[prop_name] if x_has_prop else val_if_missing

def safe_getprop(x, prop_name, val_if_missing=None, val_if_error=None):
    try:
        return getprop(x, prop_name, val_if_missing)
    except Exception, e:
        return val_if_missing if val_if_error is None else val_if_error

def get_dict_prop(x, dict_name, prop_name_within_dict, val_if_missing=None):
    """
    If dict_name is a property of x whose value is a dict, 
    returns the indicated property value from within that dict.  
    Otherwise, val_if_missing.
    """
    the_dict = getprop(x, dict_name)
    if the_dict is not None and prop_name_within_dict in the_dict:
        return the_dict[prop_name_within_dict]
    else:
        return val_if_missing



def getprop_chain(orig_x, *prop_seq, **kwargs):
    """
    Follow the given chain of properties, starting from orig_x. 
    Example:

        >>> x = { 'a' : { 'b': { 'c' : 'surprise!' } } }
        >>> getprop_chain(x, 'a', 'b')
        {'c': 'surprise!'}

    Optional keyword arguments:
    val_if_missing : value to return if one of the properties is missing
              safe : swallow exceptions -- e.g.,
                        >>> getprop_chain(6, 'a', safe=True) # ==> None

     short_circuit : return val_if_missing immediately upon encountering
                     first missing property.  otherwise, will 
                     try to retrieve the next property from that value.
    """
    return getprop_sequence(orig_x, prop_seq, **kwargs)


def getprop_sequence(orig_x, prop_seq,
                     val_if_missing=None, safe=False, short_circuit=True):
    """
    Exactly like getprop_chain, but property chain is a single list argument.
    Example:

        >>> getprop_sequence(x, ['a', 'b'])
        {'c': 'surprise!'}
    """
    x = orig_x
    der_proppengetten = safe_getprop if safe else getprop
    __SENTINEL__ = object()     # guaranteed unique!
    for prop in prop_seq:
        x = der_proppengetten(x, prop, val_if_missing=__SENTINEL__)
        if x == __SENTINEL__ :
            x = val_if_missing
            if short_circuit:
                break
    return x


def getprop_star(orig_x, prop_path,
                 val_if_missing=None, safe=False, short_circuit=True):
    """
    Follow the "property path", starting from orig_x. 
    Example:

        >>> x = { 'a' : { 'b': { 'c' : 'surprise!' } } }
        >>> getprop_star(x, 'a.b')
        {'c': 'surprise!'}

    """
    return getprop_sequence(orig_x, prop_path.split('.'),
        val_if_missing=val_if_missing,
        safe=safe, short_circuit=short_circuit)


def setprop_star(orig_x, prop_path, new_val):
    prop_path_components = prop_path.split('.')
    x = orig_x
    for prop in prop_path_components[:-1] :
        last_x = x
        x = getprop(x, prop)
        if x is None:
            last_x[prop] = x = {}
    x[ prop_path_components[-1] ] = new_val
    return orig_x


def make_property_overlayer(props_n_vals, propval_callback=None, ignore_nones=False):
    def overlay_properties(substrate_obj):
        for k,v in props_n_vals.iteritems():
            old_val = getprop_star(substrate_obj, k)
            if v is not None or not ignore_nones:
                do_if_doable(propval_callback, k, old_val, v)
                setprop_star(substrate_obj, k, v)
        return substrate_obj
    return overlay_properties


DEFAULT_HERITAGE_PATH = {'object': 'parent'}

def get_inheritable_prop(obj, prop_name, heritage_path=None, no_value=None):
    """
    Returns O[prop_name], where O == the 'obj' argument or its nearest ancestor.

    "Nearest ancestor" is defined as a walk up the object graph along some set 
    of object properties, which set is specified by 'heritage_path'.
    
    By default, each object points to its parent via the property 'parent'.
    """
    heritage_path = heritage_path or DEFAULT_HERITAGE_PATH
    if obj is None:
        raise Exception("Cannot get properties from None object!")
    elif obj.has_key(prop_name):
        return obj[prop_name]
    else:
        no_value = ((lambda v: v is None) if no_value is None else no_value)
        for iama in obj.__class__.__mro__:
            iama_type_fqn = (("" if iama.__module__ == '__builtin__' else
                              (iama.__module__ + "."))
                             +
                             iama.__name__)

            # HERITAGE_PATH keys may be <type 'type'> objects or fully qualified class names
            if heritage_path.has_key(iama):
                inherit_via = heritage_path[iama]
            elif heritage_path.has_key(iama_type_fqn):
                inherit_via = heritage_path[iama_type_fqn]
            else:
                continue

            # HERITAGE_PATH values may be functions to apply to obj, or attribute names within obj
            if inspect.isroutine(inherit_via):
                parent_maybe = inherit_via(obj)
            elif isinstance(inherit_via, basestring):
                parent_maybe = obj[inherit_via] if obj.has_key(inherit_via) else None
            else:
                raise Exception("Don't understand how to inherit from a %s via %s" %
                                (iama_type_fqn, inherit_via))

            if parent_maybe is None or no_value(parent_maybe):
                continue
            else:
                return get_inheritable_prop(parent_maybe, prop_name, heritage_path)

    return None                 # live to recurse another day.


# yoinked from : 
# http://stackoverflow.com/questions/3012421/python-lazy-property-decorator

def lazyprop(fn):
    attr_name = '_lazy_' + fn.__name__
    @property
    def _lazyprop(self):
        if not hasattr(self, attr_name):
            setattr(self, attr_name, fn(self))
        return getattr(self, attr_name)
    return _lazyprop


################################################################################
# A dash of higher-order functional programming
################################################################################

def make_getprop(prop_name, val_if_missing=None):
    """
    Returns a procedure that returns the given property of its argument.
    """
    return lambda x: getprop(x, prop_name, val_if_missing)


def prop_equals(prop_name, val):
    """
    Returns a procedure that returns True iff its argument's value for the given property is VAL
    """
    return lambda x: prop_name in x and x[prop_name] == val


def is_equal(y):
    return lambda x: x == y


def find(f, seq, exc_msg=None):
    """
    Return first item in sequence for which f(item) tests True.
    """
    result_ish = or_map(lambda x: f(x) and [x], seq, exc_msg=exc_msg)
    return result_ish[0] if result_ish else None


def or_map(f, seq, exc_msg=None):
    """
    Return first result f(item) in sequence that tests True;
    else returns the last result f(item)
    """
    maybe_result = None
    for item in seq:
        maybe_result = f(item)
        if maybe_result:
            return maybe_result

    if exc_msg:
        raise Exception(exc_msg)
    else:
        return maybe_result


def partition(has_it, items):
    """
    Returns a 2-tuple containing the list of items satisfying
    and the list of items not satisfying the predicate has_it.
    """
    haves = []
    have_nots = []
    for i in items:
        if has_it(i):
            haves.append(i)
        else:
            have_nots.append(i)
    return ( haves, have_nots )


#################### dict vs. tuple #####################################
##
## Transform simple dict objects into something hashable, and back again.
##
## Not indicated for use with dicts having complex (unhashable) values.
##

def dict_to_tuple(sd):
    """
    >>> dict_to_tuple({'a':1, 'b':2})
    (('a', 1), ('b', 2))
    >>> dict_to_tuple({'a':1, 'b':2}) == dict_to_tuple({'b':2, 'a':1})
    True
    """
    return tuple((k,v) for k,v in sorted(sd.items()))

def tuple_to_dict(tp):
    """
    >>> x = tuple_to_dict((('a', 1), ('b', 2)))
    >>> x['b']
    2
    """
    return dict(tp)

#################### compose_dicts ######################################

def compose_dicts(*dict_list):
    result = {}
    for this_dict in dict_list:
        result.update(this_dict)
    return result


#################### extract_map ########################################

def extract_map( things,
                 key_extractor=None,
                 keys_extractor=None,
                 value_extractor=None,
                 values_extractor=None,
                 value_accumulator=None,
                 thing_summarizer=str,
                 knil=None,
                 exclude_none_key=False,
                 result=None ):
    result = {} if result is None else result
    thing2vals = _make_thing2stuffs( value_extractor, values_extractor )
    thing2keys = _make_thing2stuffs( key_extractor, keys_extractor )
    accumulate = ( value_accumulator if value_accumulator is not None else
                   lambda v_old, v_new: v_old + v_new )
    for thing in things:
        try:
            vals = thing2vals( thing )
            for k in thing2keys( thing ) :
                if k is None and exclude_none_key:
                    continue
                for v in vals :
                    if k in result :
                        result[k] = accumulate( result[k], v )
                    elif knil is not None:
                        result[k] = accumulate( knil, v )
                    else :
                        result[k] = v
        except Exception, e:
            (et, ev, etb) = sys.exc_info()
            print "Problem with %s : %s" % (thing_summarizer(thing), str(e))
            raise e, None, etb
    return result


def _make_thing2stuffs( stuff_ex, stuffs_ex ):
    if stuffs_ex is not None:
        return stuffs_ex
    elif stuff_ex is not None:
        return lambda thng: listify( stuff_ex( thng ) )
    else:
        return listify

# this is less clear than the previous?
def _make_thing2stuffzes( stuff_ex, stuffs_ex ):
    return ( stuffs_ex if stuffs_ex is not None else
             listify   if stuff_ex  is None else
             lambda thng: [ stuff_ex( thng ) ] )

################################################################################
def listify( thing ):
    return [ thing ]


################################################################################
# Robustification: functions & decorators to auto-retry failure-prone operations
################################################################################

from functools import wraps

def robustify(**retry_kwargs):
    """
    This decorator factory produces a decorator which wraps its decorated
    function with retry_till_done() (q.v.), invoked according to the given
    optional keyword arguments.

    >>> y = 3
    >>> #noinspection PyUnresolvedReferences
    >>> @robustify(max_attempts=3, failure_val="drat")
    ... def foo(x):
    ...     print "i think i can"
    ...     global y
    ...     y += x
    ...     if y < 10:
    ...         raise Exception("not there yet")
    ...     return y
    >>> foo(1)
    i think i can
    i think i can
    i think i can
    'drat'
    >>> foo(3)
    i think i can
    i think i can
    12

    Robustification ==> the Exception was never permitted to escape.
    """
    def robustificator(f):
        @wraps(f)
        def f_robustified(*args, **kwargs):
            return retry_till_done(lambda: f(*args, **kwargs),
                **retry_kwargs)
        return f_robustified
    return robustificator


################################################################################

def robustify_methods(**retry_kwargs):
    original_methods = {}
    def method_robustificator(methname):
        orig_f = original_methods[methname]
        def f_robustified(*args, **kwargs):
            do_the_stuff = lambda: orig_f(*args, **kwargs)
            return retry_till_done(do_the_stuff, **retry_kwargs)
        return f_robustified

    def class_robustificator(c):
        for method_name in filter(lambda n: hasmethod(c, n), dir(c)):
            oldmeth = getattr(c, method_name)
            original_methods[method_name] = oldmeth   # pyclosure ftl
            newmeth = method_robustificator(method_name)
            newmeth.__name__ = method_name + "_robustified"
            newmeth.__doc__ = (None if oldmeth.__doc__ is None else
                               oldmeth.__doc__ + "\n (Robustified)")
            setattr(c, method_name, newmeth)
        return c
    return class_robustificator

################################################################################
def retry_till_done(do_it, is_done=None,
                    max_wait_in_secs=None, max_attempts=None,
                    do_on_exception=True, do_on_error=None,
                    success_val=None, failure_val=False,
                    do_on_failure=None,
                    do_between_attempts=None, retry_interval=5):
    """
    Repeatedly calls do_it() until it succeeds or bails.

    Succeeds ==> does not raise an Error or Exception, and (if is_done is given)
                 is_done() is true.
              ==> Returns success_val if given (non-None), or else 
                  [default] returns the value from the last do_it() call.

    Bails ==> an Error or Exception is not handled, or (if they are given)
              one of max_attempts or max_wait_in_secs has been exceeded.
              ==> Runs do_on_failure() if it is given, and then
              ==> Returns failure_val if given, or else the last do_it() return
              ==> Returns the last do_it() return if failure_val is None, or else
                  [default] returns failure_val itself -- by default: False.

    Waits retry_interval seconds between do_it() attempts, after having
    run do_between_attempts() if that function is given..

    Errors and Exceptions are caught and handled according to 
    do_on_error and do_on_exception, respectively.  If the value is:
              True            ==> keep on truckin' (silently mask)
              None or False   ==> re-raise
              a function FOO  ==> run FOO() and continue
    
    The default behavior is to re-raise Errors, and silently mask Exceptions.
    """
    num_attempts = 0
    start_time = now()
    succeeded = False
    how_was_it = None

    def had_enough():
        return ((max_attempts is not None and num_attempts >= max_attempts) or
                (max_wait_in_secs is not None and now() - start_time >= max_wait_in_secs))
    def handle_via(do_handling, *args):
        if isinstance(do_handling, bool) and do_handling:
            pass            # True ==> persevere through all that
        elif not do_handling:
            raise           # None/False ==> let that propagate back to caller
        else:
            do_if_doable(do_handling, *args)

    while not had_enough():
        try:
            if num_attempts > 0:
                do_if_doable(do_between_attempts)
                sleep(retry_interval)
            num_attempts += 1
            how_was_it = do_it()
            succeeded = (is_done is None or is_done())
            if succeeded:
                break
        except Exception, e:
            handle_via(do_on_exception, e)
        except:
            handle_via(do_on_error)

    if succeeded :
        return how_was_it if success_val is None else success_val
    else :
        do_if_doable(do_on_failure)
        return how_was_it if failure_val is None else failure_val


def do_if_doable(doable_or_none, *args):
    if is_applicable(doable_or_none):
        return doable_or_none(*args)

def is_applicable(doable_or_none):
    return type(doable_or_none) in [ types.FunctionType, types.MethodType,
                                     types.BuiltinFunctionType,
                                     types.BuiltinMethodType ]

################################################################################
# pyflection
################################################################################

# credit:
#   http://stackoverflow.com/questions/1091259/how-to-test-if-a-class-attribute-is-an-instance-method
#
def hasmethod(kls, name):
    return hasattr(kls, name) and type(getattr(kls, name)) == types.MethodType
