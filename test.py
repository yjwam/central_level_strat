import schedule as sd
import time

def dothis(h):
    h-=1
    print("Hour left : "+str(h))
    return h

h=24
d=":00"
sd.every().minute.at(d).do(dothis,h)
a = dothis(h)
print(a)
while True:
    sd.run_pending()
    return_value = a
    print(return_value)
    time.sleep(1)