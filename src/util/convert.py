#########################################################################
# convert.py
# ----------
# 
# @author Theodore Mui
# @email theodoremui@gmail.com
# @date Sun Feb 12 16:34:09 PST 2023
#
#########################################################################


def safeFloat(string):
    try:
        return float(string)
    except ValueError:
        return None

