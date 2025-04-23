from machine import UART
from modem import getDevImei
from utime import sleep
import ujson as json
from umqtt import MQTTClient
from machine import Pin
import uos
from misc import Power
import checkNet
from sim import getStatus
import sms

class mb485_gw():
    def __init__(self):
        self.serial = UART(UART.UART2, 115200, 8, 0, 1, 0) 
        self.channels = ["","","","",""]
        self.dev_id = getDevImei()
        self.serial.set_callback(self.u_cll_back)
        self.clientid = str(self.dev_id)
        self.mqtt_server = ""
        self.port = 0
        self.username=''
        self.password=''
        self.subtopic=""
        self.polling=1000
        self.ssl=False
        self.mqttcl = None
        self.pubflag=True
        self.conf=False
        self.loadInit()
        sms.setCallback(self.cb)
        self.gpiox = Pin(Pin.GPIO42, Pin.OUT, Pin.PULL_DISABLE, 0)
        self.cnt=0
        # self.pubtopic = "testtopic/hashdev_1"


    def loadInit(self):
        try:
            ds = self.filerOp('conf')
            data = json.loads(ds)
            self.conf=True
        except:
            data = {"URL":"broker.emqx.io","PORT":1883,"USER_ID":"","PASSWORD":"","keepalive" : 120,"SUBTOPIC":"hashsub/"+str(self.dev_id)[-4:],"SSL":0,"POLLING":1000}

        self.mqtt_server = data["URL"]
        self.port = data["PORT"]
        self.username=data["USER_ID"]
        self.password=data["PASSWORD"]
        self.subtopic=data["SUBTOPIC"]
        self.ssl=data["SSL"]
        self.polling=data["POLLING"]

    def cb(self,            args):
        index = args[1]
        storage = args[2]
        # print('New message! storage:{},index:{}'.format(storage, index))
        mss=sms.searchTextMsg(index)
        message=mss[1]
        print(message)
        
        if(message=="FORMAT"):
            self.serial.write("HLT\r\n")
            self.clearConf()
            
        
        flag=True
        try:
           
            dx = json.loads(message)
        except:
            sms.deleteMsg(index)
        else:
            print(dx)
            
            for x in ["URL","PORT","USER_ID","PASSWORD","SUBTOPIC","SSL","POLLING"]:
                if x not in dx.keys():
                    flag=False
            if  flag:
                self.serial.write("HLT\r\n")
                self.filewOp('conf',message)
                Power.powerRestart()
            
        

        sms.deleteMsg(index)

    def clearmssg(self):
        
        no=sms.getMsgNums()
        for i in range(0,no):
            sms.deleteMsg(i)
        


    def clearConf(self):
        print("Removing Configs")
        for x in ["conf"]:
                try:
                    print('''Removing : {}.json'''.format(x))
                    uos.remove('''/usr/{}.json'''.format(x))  
                    sleep(1) 
                except:
                    None
        print("Rebooting in 5 sec..")
        sleep(5)
        self.gpiox.write(0)
        sleep(0.5)
        Power.powerRestart()



    def publish(self,pubtopic,msg, retain=False, qos=0):
        try:   
            self.mqttcl.publish(pubtopic, msg, retain, qos)
            print(msg)
            self.gpiox.write(0)
            return 0
        except Exception as e:
            print("[WARNING] Publish failed. Try to reconnect : %s" % str(e))
            self.gpiox.write(0)
            return -1

    def connect(self,keepAlive=300, clean_session=True):
        print(str(self.clientid), str(self.mqtt_server), str(self.port), str(self.username), str(self.password),str(self.ssl))
        mqtt_client = MQTTClient(self.clientid, self.mqtt_server, self.port, self.username, self.password,keepAlive,ssl=self.ssl)
        while self.mqttcl is None:
            self.cnt=self.cnt + 1
            try:
                mqtt_client.connect(clean_session=clean_session)
                self.mqttcl= mqtt_client
                self.mqttcl.set_callback(self.sub_cb)
                self.subscribe()
                print("Connect Successful")
            except Exception as e:
                print("Retrying in 2 Second..")
                print(e)
                sleep(2)
                
                
    
    def subscribe(self,qos=0):
        print("Subscribed to: "+self.subtopic)
        self.mqttcl.subscribe(self.subtopic,qos)
    
    def sub_cb(self,topic, msg):
        print("Subscribe Recv: Topic={},Msg={}".format(topic.decode(), msg.decode()))
        dic=json.loads(msg.decode())

        if "STATUS" in dic.keys():
            if dic["STATUS"]=="HLT":
                self.serial.write("HLT\r\n")
                
            if dic["STATUS"]=="PUBLISH":
                self.serial.write("PUBLISH\r\n")
        
        if "DELAY" in dic.keys():
            self.serial.write('''delay,{}\r\n'''.format(dic["DELAY"]))
        
        if "REBOOT" in dic.keys():
            self.serial.write("HLT\r\n")
            sleep(1)
            self.mqttcl.disconnect()
            self.mqttcl.close()
            Power.powerRestart()
            
        if "DELETE" in dic.keys():
            if dic["DELETE"] !=0:
                self.serial.write('''delete,{}\r\n'''.format(dic["DELETE"]))
                try:
                    uos.remove('''/usr/{}.json'''.format(str(dic["DELETE"])))
                except:
                    None
            elif dic["DELETE"] ==0:
                try:
                    uos.remove("/usr/conf.json")
                    self.serial.write("HLT\r\n")
                    sleep(1)
                    self.mqttcl.disconnect()
                    self.mqttcl.close()
                    Power.powerRestart()
                except:
                    None
        if "CONF" in dic.keys():
            if(dic["CONF"] in [1,2,3,4,5]):
                try:
                    data=self.filerOp(dic["CONF"])
                    val=json.loads(data)
                    v=val["ID"]
                    v1='''ID:{}'''.format(self.channels[v-1])
                    v2='''ID:{}'''.format(str(v))
                    msgr=data.replace(v2,v1)
                    self.publish('''CONF_{}'''.format(self.dev_id[-4:]),msgr)
                except:
                    None
            


        self.jsDataChk(msg.decode())
        
    def u_cll_back(self,params):
        b_rec = self.serial.any()
        

        self.gpiox.write(1)
        d_Rec = self.serial.read(b_rec)
        
        d_Rec =d_Rec.decode()
        print(d_Rec)
        sd =json.loads(d_Rec)
        if "reset" in sd.keys():
            self.gpiox.write(1)
            self.clearConf()
            return 0
        num =sd["ID"]
        dax = '''"ID":{}'''.format(str(num))
        dax2 = '''"ID":"{}"'''.format(self.channels[sd["ID"]-1])
        d_Rec=d_Rec.replace(dax,dax2)
        if self.pubflag:
            self.publish(self.channels[sd["ID"]-1],d_Rec)
        
    def regcb(self):
        self.serial.set_callback(self.u_cll_back)
        sms.setCallback(self.cb)

    def sendinit(self):
        for x in [1,2,3,4,5]:
            try:
                jsdata=self.filerOp(x)
                print(jsdata)
                dx=json.loads(jsdata)
                self.channels[dx["CHANNEL_ID"]-1]=dx["PUBTOPIC"]
                self.serial.write(jsdata)
                self.serial.write('\r\n')
                print("written "+str(x))
                sleep(2)
            except:
                None   
        self.serial.write("PUBLISH\r\n")
    
    def filerOp(self,fileId):
        rStream=open("/usr/"+str(fileId)+".json", "r")
        data=rStream.read()
        rStream.close()
        print("read")
        return data
                
    def filewOp(self,fileId,data):
        rStream=open("/usr/"+str(fileId)+".json", "w")
        rStream.write(data)
        rStream.close()
        print(data)
        print("written")

        
    def jsDataChk(self,data):
        dx = json.loads(data)
        flag=True
        for x in ["URL","PORT","USER_ID","PASSWORD","SUBTOPIC","SSL","POLLING"]:
            if x not in dx.keys():
                flag=False
        if  flag:
            self.mqttcl.disconnect()
            self.mqttcl.close()
            self.pubflag=False
            self.mqtt_server = dx["URL"]
            self.port = dx["PORT"]
            self.username=dx["USER_ID"]
            self.password=dx["PASSWORD"]
            self.subtopic=dx["SUBTOPIC"]
            self.ssl=dx["SSL"]
            self.polling=dx["POLLING"]
            print(self.username)
            print(self.mqtt_server)
            print(self.port)
            print( self.subtopic)
            print( self.password)
            print("close")
            sleep(5)
            self.filewOp('conf',data)
            Power.powerRestart()
            sleep(2)
            # try:
            #     self.connect()
            #     print("trying")
            #     sleep(2)
            # except Exception as e:
            #     print(e)
            # if self.mqttcl.get_mqttsta() ==0:
            #     self.filewOp('conf',data)
            #     self.pubflag=True
            # else:
            #     try:
            #         print("not changing")
            #         self.mqttcl.disconnect()
            #         self.mqttcl.close()
            #     except:
            #         None
            #     self.loadInit()
            #     sleep(5)
            #     self.connect()
            #     self.pubflag=True
            #     sleep(2)

            return 0
        
        flag=True
        for x in ["ID","BAUD","CHANNEL_ID","FRAME","MB_DATA","PUBTOPIC"]:
            if x not in dx.keys():
                flag=False
        if  flag:
            if dx["CHANNEL_ID"] in [1,2,3,4,5]:
                self.channels[dx["CHANNEL_ID"]-1]=dx["PUBTOPIC"]
                self.filewOp(dx["CHANNEL_ID"],data)
                self.serial.write(data)
                self.serial.write('\r\n')
                print("written "+str(x))
                return 0

    def blankr(self):
        a= 0
        while True:
            
            self.mqttcl.wait_msg()
            
    
    def wait_connect(self):
        stagecode, subcode = 0,0
        while stagecode!=3 and subcode!=1:
            stagecode, subcode = checkNet.wait_network_connected(30)
            sleep(1)


gw = mb485_gw()

gw.wait_connect()
gw.clearmssg()
gw.connect()
gw.regcb()
gw.sendinit()
gw.blankr()




