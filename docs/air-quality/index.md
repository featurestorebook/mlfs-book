---
city: Blekinge and Kronoberg and Halland Counties
---

# Air Quality Dashboard

![Hopsworks Logo](../titanic/assets/img/logo.png)

{% include air-quality.html %}

{% assign aq_imgs = site.static_files | where_exp: "file", "file.path contains '/docs/air-quality/assets/img/'" %}
{% if aq_imgs.size > 0 %}
{% for image in aq_imgs %}
{% assign base = image.name | split: '.' | first %}
{% assign parts = base | split: '_' %}
{% assign title = "" %}
{% for part in parts %}
{% assign title = title | append: part | capitalize %}
{% if forloop.last == false %}
{% assign title = title | append: " " %}
{% endif %}
{% endfor %}
**{{ title }}**

![{{ title }}]({{ image.path | relative_url }})
{% endfor %}
{% else %}

> No air-quality dashboard images found in `docs/air-quality/assets/img`. Run the batch inference notebook to generate them.
> {% endif %}

There is also a Python program to interact with the air quality ML system using language (text, voice),
powered by a [function-calling LLM](https://www.hopsworks.ai/dictionary/function-calling-with-llms).
