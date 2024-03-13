import gradio as gr
from transformers import pipeline
import numpy as np
import hopsworks
from xgboost import XGBRegressor
from functions.llm_chain import load_model, get_llm_chain, generate_response

# Initialize the ASR pipeline
transcriber = pipeline("automatic-speech-recognition", model="openai/whisper-base.en")

def connect_to_hopsworks():
    # Initialize Hopsworks feature store connection
    project = hopsworks.login(
        host="snurran.hops.works",
        project="AirQuality_Book",
    )
    fs = project.get_feature_store()
    
    # Retrieve the model registry
    mr = project.get_model_registry()

    # Retrieve the 'air_quality_fv' feature view
    feature_view = fs.get_feature_view(name="air_quality_fv", version=2)

    # Initialize batch scoring
    feature_view.init_batch_scoring(1)
    
    # Retrieve the 'air_quality_xgboost_model' from the model registry
    retrieved_model = mr.get_model(name="air_quality_xgboost_model", version=1)

    # Download the saved model artifacts to a local directory
    saved_model_dir = retrieved_model.download()

    model_air_quality = XGBRegressor()
    model_air_quality.load_model(saved_model_dir + "/model.json")
    
    return feature_view, model_air_quality


def retrieve_llm_chain():
    model_llm, tokenizer = load_model()
    llm_chain = get_llm_chain(model_llm, tokenizer)
    return model_llm, tokenizer, llm_chain

# Setup the models and feature view
feature_view, model_air_quality = connect_to_hopsworks()
model_llm, tokenizer, llm_chain = retrieve_llm_chain()

def transcribe(audio):
    sr, y = audio
    y = y.astype(np.float32)
    if y.ndim > 1 and y.shape[1] > 1:
        y = np.mean(y, axis=1)
    y /= np.max(np.abs(y))
    return transcriber({"sampling_rate": sr, "raw": y})["text"]

def generate_query_response(user_query):
    response = generate_response(
        user_query,
        feature_view,
        model_llm,
        tokenizer,
        model_air_quality,
        llm_chain,
        verbose=False,
    )
    return response

def handle_input(text_input=None, audio_input=None):
    if audio_input is not None:
        user_query = transcribe(audio_input)
    else:
        user_query = text_input
    
    if user_query:
        return generate_query_response(user_query)
    else:
        return "Please provide input either via text or voice."

iface = gr.Interface(
    fn=handle_input,
    inputs=[gr.Textbox(placeholder="Type here or use voice input..."), gr.Audio()],
    outputs="text",
    title="🌤️ AirQuality AI Assistant 💬",
    description="Ask your questions about air quality or use your voice to interact."
)

iface.launch(share=True)
