from distutils.log import error
from os import wait
from tracemalloc import stop
from urllib.request import urlretrieve
from azure.cognitiveservices.vision.face import FaceClient
from msrest.authentication import CognitiveServicesCredentials
from person import Person
import glob
import zipfile
import time
import asyncio
OUTPUT_DIR = "./DownloadFile/"
INPUT_FILE_SAVE_DIR = "./DownloadedFile.zip"
SOURCE_NAME = './PeopleSourceImgs/'
# This key will serve all examples in this document.
KEY = "c6ef2d6b5bd346b0ada9ec7ee6d397e3"
# This endpoint will be used in all examples in this quickstart.
ENDPOINT = "https://aokugeu.cognitiveservices.azure.com/"


class DataProcessor:

    # constuctor
    def __init__(self, url=''):
        self.url = url
        self.face_client = FaceClient(
            ENDPOINT, CognitiveServicesCredentials(KEY))
        self.persons = {}
        self.timer = 0

    def download_and_process_file(self) -> bool:
        try:
            urlretrieve(self.url, INPUT_FILE_SAVE_DIR)
            if not zipfile.is_zipfile(INPUT_FILE_SAVE_DIR):
                return False

            with zipfile.ZipFile(INPUT_FILE_SAVE_DIR) as zipObj:
                zipObj.extractall(OUTPUT_DIR)

            return True
        except:
            return False

    def execute(self):
        self.detect_face()
        return False

    def detect_face(self):
        target_map = self.generateMap(OUTPUT_DIR, SOURCE_NAME)
        for k, v in target_map.items():
            customer_id = k.split('.')[0]
            if len(v) <= 0:
                curpeople = self.persons.get(customer_id)
                if curpeople:
                    curpeople.set_fraud(0)
                continue
            k = k.split('.')[0]
            key_user = self.get_person(k)
            key_user_file = key_user.get_account_id() if key_user is not None else ""
            if key_user_file == "":
                return
            key_user_file = k + "_" + key_user_file + ".jpg"
            target_id = self.use_api(OUTPUT_DIR+key_user_file)
            souce_ids = [self.use_api(SOURCE_NAME+x) for x in v]
            myflag = False

            for c in souce_ids:
                try:
                    verify_result_same = self.face_client.face.verify_face_to_face(
                        c, target_id)
                    if verify_result_same.is_identical:
                        self.persons.get(customer_id).set_fraud(1)
                        myflag = True
                        break
                except:
                    continue;
            if myflag == False:
                self.persons.get(customer_id).set_fraud(0)

    def use_api(self, name):
        try:
            '''
            if self.timer == 9:
                time.sleep(1)
                self.timer = 0
            self.timer += 1
            '''
            image = open(name, 'r+b')
            detected_faces1 = self.face_client.face.detect_with_stream(image, detectionModel='deteion_03')
            if detected_faces1 is not None and len(detected_faces1) >= 1:
                return detected_faces1[0].face_id
        except:
            return "empty"


    def generateMap(self, target_names, souce_names):
        target_names = glob.glob(target_names+"*")
        target_names_map = {}
        for x in target_names:
            x_filename = x.split('/')[-1]
            x_filename = x_filename.split('.')[0]
            x_filename = x_filename.split('_')
            cur_person = Person(x_filename[0], x_filename[1])
            self.add_person(cur_person)
            target_names_map[x_filename[0]+'.jpg'] = []

        souce_names = glob.glob(souce_names+"*")

        for souce in souce_names:
            souce = souce.split('/')[-1]
            cur = souce[0:4] + '.jpg'
            if cur in target_names_map:
                target_names_map[cur].append(souce)
        return target_names_map

    def add_person(self, person):
        customer_id = person.get_customer_id()
        if customer_id not in self.persons:
            self.persons[customer_id] = person

    def get_person(self, id):
        if id in self.persons:
            return self.persons.get(id)
        return ""
