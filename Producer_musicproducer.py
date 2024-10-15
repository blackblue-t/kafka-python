#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from confluent_kafka import Producer
import sys
import time
import random


# 用來接收從Consumer instance發出的error訊息
def error_cb(err):
    print('Error: %s' % err)


# 主程式進入點
if __name__ == '__main__':
    # 步驟1. 設定要連線到Kafka集群的相關設定
    props = {
        # Kafka集群在那裡?
        'bootstrap.servers': 'localhost:9092',  # <-- 置換成要連接的Kafka集群
        'error_cb': error_cb  # 設定接收error訊息的callback函數
    }
    # 步驟2. 產生一個Kafka的Producer的實例
    producer = Producer(props)
    # 步驟3. 指定想要發佈訊息的topic名稱
    topicName = 'ak03.musictest'
    msgCount = 10000
    listener_list = ['Hanson','Jason','Mark',"Frank","Julia","Emily","Kristina"]
    music_list = ["pop music","rap","hiphop","death metal","rock","jazz"]


    try:
        print('Start sending messages ...')
        # produce(topic, [value], [key], [partition], [on_delivery], [timestamp], [headers])
        for i in range(msgCount):
            listener = random.choice(listener_list)
            music = random.choice(music_list)

            producer.produce(topicName, value=f"{listener} - {music}")

            producer.poll(0)  # <-- (重要) 呼叫poll來讓client程式去檢查內部的Buffer
            print(f"{listener} is listen {music}")
            time.sleep(0.5)  # 讓主執行緒停個0.5秒

        print('Send ' + str(msgCount) + ' messages to Kafka')
    except BufferError as e:
        # 錯誤處理
        sys.stderr.write(
            '%% Local producer queue is full ({} messages awaiting delivery): try again\n'.format(
                len(producer)
            ))
    except Exception as e:
        sys.stderr.write(str(e))
    # 步驟5. 確認所有在Buffer裡的訊息都己經送出去給Kafka了
    producer.flush(10)
    print('Message sending completed!')
