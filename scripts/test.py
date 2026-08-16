import gspread
import os
import certifi
import logging
from translate import Translator

import argostranslate.package
import argostranslate.translate

# Download and install language pair (e.g., English to Spanish)
def install():

    argostranslate.package.update_package_index()
    available_packages = argostranslate.package.get_available_packages()
    package_to_install = next(
        filter(
            lambda x: x.from_code == from_code and x.to_code == to_code, available_packages
        )
    )
    argostranslate.package.install_from_path(package_to_install.download())

# Translate instantly offline
from_code = "en"
to_code = "zh"
install()
translatedText = argostranslate.translate.translate("Hello World", from_code, to_code)
print(translatedText)

# 1. 設定日誌格式：包含 [時間] [層級] 檔案名稱:行數 - 訊息
logging.basicConfig(
    level=logging.INFO,  # 設定最低捕捉層級
    format='%(asctime)s [%(levelname)s] %(filename)s:%(lineno)04d - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'  # 精簡時間格式
)

def dummy():
    gc = gspread.service_account(filename='.service_account.json')
    sh = gc.open("HK-STOCKS-ANALYSIS")
    # sh.update_acell("A1", "Hello World")

def translate():
    # Set up the translator destination language
    translator = Translator(to_lang="zh")

    # Translate text
    translation = translator.translate("Electronic Gaming & Multimedia")

    print(translation)  # Output: Hola, ¿cómo estás?


# translate()
# dummy()    