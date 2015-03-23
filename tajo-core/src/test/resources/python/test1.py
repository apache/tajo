from pig_util import outputSchema

@outputSchema('value:int')
def return_one():
    return 1
