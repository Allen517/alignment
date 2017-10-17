# -*- coding:utf8 -*-

def unicode2utf8(val):
	if isinstance(val, unicode):
		return val.encode('utf8')
	else:
		return val

def property_format(prop):
	return "'"+prop+"'" if isinstance(prop, str) else prop

def condition_clause_format(variable, props):
	condition_clause = ""
	for k,v in props.iteritems():
		if 'label' in k:
			continue
		condition_clause += "{}.{}{} AND ".format(variable,k,v)
	if condition_clause:
		return condition_clause[:-5]

def fuzzy_condition_clause_format(variable, props):
	condition_clause = ""
	for k,v in props.iteritems():
		if 'label' in k:
			continue
		condition_clause += "{}.{}=~'.*{}.*' AND ".format(variable,k,v)
	if condition_clause:
		return condition_clause[:-5]
	return ""

def acc_condition_clause_format(variable, props):
	condition_clause = ""
	for k,v in props.iteritems():
		if 'label' in k:
			continue
		condition_clause += "{}.{}{} AND ".format(variable,k,property_format(v))
	if condition_clause:
		return condition_clause[:-5]
	return ""

import warnings
import functools

def deprecated(replacement=None):
    """A decorator which can be used to mark functions as deprecated.
    replacement is a callable that will be called with the same args
    as the decorated function.

    >>> @deprecated()
    ... def foo(x):
    ...     return x
    ...
    >>> ret = foo(1)
    DeprecationWarning: foo is deprecated
    >>> ret
    1
    >>>
    >>>
    >>> def newfun(x):
    ...     return 0
    ...
    >>> @deprecated(newfun)
    ... def foo(x):
    ...     return x
    ...
    >>> ret = foo(1)
    DeprecationWarning: foo is deprecated; use newfun instead
    >>> ret
    0
    >>>
    """
    def outer(fun):
        msg = "psutil.%s is deprecated" % fun.__name__
        if replacement is not None:
            msg += "; use %s instead" % replacement
        if fun.__doc__ is None:
            fun.__doc__ = msg

        @functools.wraps(fun)
        def inner(*args, **kwargs):
            warnings.warn(msg, category=DeprecationWarning, stacklevel=2)
            return fun(*args, **kwargs)

        return inner
    return outer