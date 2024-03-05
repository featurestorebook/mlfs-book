import gradio as gr
import numpy as np
from PIL import Image
import requests

import hopsworks
import joblib

project = hopsworks.login()
fs = project.get_feature_store()


mr = project.get_model_registry()
model = mr.get_model("titanic_modal", version=1)
model_dir = model.download()
model = joblib.load(model_dir + "/titanic_model.pkl")


def titanic(Sex, Age, Pclass, Fare, Parch, SibSp, Embarked):
    input_list = []
    input_list.append(Sex)
    input_list.append(Age)
    input_list.append(Pclass + 1.0)
    input_list.append(Fare)
    input_list.append(Parch)
    input_list.append(SibSp)
    input_list.append(Embarked)
    res = model.predict(np.asarray(input_list).reshape(1, -1))
    pic_url = "https://raw.githubusercontent.com/featurestorebook/mlfs-book/main/docs/assets/img/titanic_" + str(res[0]) + ".jpg"
    img = Image.open(requests.get(pic_url, stream=True).raw)
    return img

demo = gr.Interface(
    fn=titanic,
    title="Titanic Passenger Survival Predictive Analytics",
    description="Enter values for an imaginary passenger and the model will predict whether he/she survived or not.",
        allow_flagging="never",
    inputs=[
        gr.Dropdown(choices=["male","female"],type='index', label="Sex"),
        gr.Slider(minimum=1.0,maximum=100.0, step=1.0, label="Age"),
        gr.Dropdown(choices=["First", "Second","Third"],type='index', label="Ticket class"),
        gr.Number(label="Fare ($)"),
        gr.Number(label="Number of parents/children aboard"),
        gr.Number(label="Number of siblings/spouses aboard"),
        gr.Dropdown(choices=["S","C", "Q"],type='index', label="Port of Embarkation")
        ],
    outputs=gr.Image(type="pil"),
    examples=[
        ["female", 30, "First", 22.10, 0.0, 0.0, "S"],
        ["male", 30, "Second", 8.11, 1.0, 1.0, "Q"],
    ],
    )

demo.launch()
