# -*- coding:utf8 -*-

def property_format(prop):
	return "'"+prop+"'" if isinstance(prop, str) else prop

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
		condition_clause += "{}.{}={} AND ".format(variable,k,property_format(v))
	if condition_clause:
		return condition_clause[:-5]
	return ""