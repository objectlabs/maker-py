__author__ = 'abdul'


from maker import o
####################################
class Person(object):
    first_name = None
    last_name = None
####################################


obj =  o({"_type":"maker_examples.Person",
                   "first_name" : "Abdul",
                   "last_name": "",
                   "kosss": 1})

print obj
