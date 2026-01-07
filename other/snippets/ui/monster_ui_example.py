from fasthtml.common import *
from monsterui.all import *

# Choose a theme color (blue, green, red, etc)
hdrs = Theme.blue.headers()

# Create your app with the theme
app, rt = fast_app(hdrs=hdrs)

@rt
def index():
    socials = (('github','https://github.com/AnswerDotAI/MonsterUI'),
               ('twitter','https://twitter.com/isaac_flath/'),
               ('linkedin','https://www.linkedin.com/in/isaacflath/'))
    return Titled("Your First App",
        Card(
            H1("Welcome!"),
            P("Your first MonsterUI app", cls=TextPresets.muted_sm),
            P("I'm excited to see what you build with MonsterUI!"),
            footer=DivLAligned(*[UkIconLink(icon,href=url) for icon,url in socials])))

serve()
