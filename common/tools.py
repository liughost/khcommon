def str_value( obj, key, default=""):
        if key in obj:
            return str(obj[key])
        else:
            return default

def int_value( obj, key, default=0):
    if key in obj:
        try:
            return int(obj[key])
        except:
            return default
    else:
        return default


def float_value( obj, key, default=0.0):
    if key in obj:
        try:
            return float(obj[key])
        except:
            return default
    else:
        return default