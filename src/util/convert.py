def safeFloat(string):
    try:
        return float(string)
    except ValueError:
        return None

