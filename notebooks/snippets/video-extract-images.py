import cv2
import os



def process_video(uploaded_video, name, surname, interval_ms):
    try:
        video_source = uploaded_video
        if video_source is None:
            return "No video file provided.", []

        folder_name = f"{name}_{surname}"
        os.makedirs(folder_name, exist_ok=True)

        # Video processing logic
        # Use video_source directly as it's a file path (string)
        temp_video_path = video_source
        face_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + 'haarcascade_frontalface_default.xml')
        vidcap = cv2.VideoCapture(temp_video_path)
        if not vidcap.isOpened():
            raise Exception("Failed to open video file.")

        fps = vidcap.get(cv2.CAP_PROP_FPS)
        frame_interval = int(fps * (interval_ms / 1000))
        frame_count = 0
        saved_image_count = 0
        success, image = vidcap.read()
        image_paths = []

        while success and saved_image_count < 86:
          if frame_count % frame_interval == 0:
            gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
            faces = face_cascade.detectMultiScale(gray, 1.2, 4)
            for (x, y, w, h) in faces:
                # Additional checks for face region validation
                aspect_ratio = w / h
                if aspect_ratio > 0.75 and aspect_ratio < 1.33 and w * h > 4000:  # Example thresholds
                    face = image[y:y+h, x:x+w]
                    face_resized = cv2.resize(face, (160, 160))
                    image_filename = os.path.join(folder_name, f"{name}_{surname}_{saved_image_count:04d}.png")
                    cv2.imwrite(image_filename, face_resized)
                    image_paths.append(image_filename)
                    saved_image_count += 1
                if saved_image_count >= 86:
                    break

          success, image = vidcap.read()
          frame_count += 1


        vidcap.release()

    except Exception as e:
        return f"An error occurred: {e}", []


def extract_video_from_ui():
    


# Gradio Interface
with gr.Blocks() as demo:
    gr.Markdown("### Video Face Detector and Uploader")
    with gr.Row():
        with gr.Column():
            video = gr.File(label="Upload Your Video")
            
        with gr.Column():
            name = gr.Textbox(label="Name")
            surname = gr.Textbox(label="Surname")
            interval = gr.Number(label="Interval in milliseconds", value=100)
            submit_button = gr.Button("Submit")
        with gr.Column():
            gallery = gallery = gr.Gallery(
        label="Generated images", show_label=False, elem_id="gallery"
    , columns=[3], rows=[1], object_fit="contain", height="auto")

    submit_button.click(
        fn=extract_video_from_ui,
        inputs=[video, name, surname, interval],
        outputs=[gr.Text(label="Result"), gallery]
    )

# CSS for styling (optional)
css = """
body { font-family: Arial, sans-serif; }
"""

demo.launch()
