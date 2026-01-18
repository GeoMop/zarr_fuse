import panel as pn
from bokeh.util.serialization import make_globally_unique_id

js_files = {
    'jquery': 'https://code.jquery.com/jquery-1.11.1.min.js',
    'goldenlayout': 'https://golden-layout.com/files/latest/js/goldenlayout.min.js'
}
css_files = [
    'https://golden-layout.com/files/latest/css/goldenlayout-base.css',
    'https://golden-layout.com/files/latest/css/goldenlayout-dark-theme.css'
]

pn.extension(js_files=js_files, css_files=css_files, design='material', theme='dark', sizing_mode="stretch_width")

# Declare Panel components
controller = pn.Column(
    pn.widgets.Select(name='Dataset', value='Mock A', options={'Mock A': 'data_a', 'Mock B': 'data_b'}),
    pn.widgets.Toggle(name='Toggle Option', value=False),
    pn.widgets.IntSlider(name='Slider', start=1, end=10, value=5),
    pn.panel("This app demos **advanced layout** using [Panel](https://panel.holoviz.org/) and [GoldenLayout](https://golden-layout.com/).", margin=(5,15)),
    pn.layout.VSpacer(),
)

# Placeholder panes (no data/plots)
top_left = pn.pane.Markdown("## Top Left View\n\nPlaceholder for first view", sizing_mode='stretch_both')
top_right = pn.pane.Markdown("## Top Right View\n\nPlaceholder for second view", sizing_mode='stretch_both')
bottom_left = pn.pane.Markdown("## Bottom Left View\n\nPlaceholder for third view", sizing_mode='stretch_both')
bottom_right = pn.pane.Markdown("## Bottom Right View\n\nPlaceholder for fourth view", sizing_mode='stretch_both')

# Set up template
template = """
{%% extends base %%}
<!-- goes in body -->
{%% block contents %%}
{%% set context = '%s' %%}
{%% if context == 'notebook' %%}
    {%% set slicer_id = get_id() %%}
    <div id='{{slicer_id}}'></div>
{%% endif %%}
<style>
:host {
    width: auto;
}
</style>

<script>
var config = {
    settings: {
        hasHeaders: true,
        constrainDragToContainer: true,
        reorderEnabled: true,
        selectionEnabled: false,
            popoutWholeStack: false,
        blockedPopoutsThrowError: true,
        closePopoutsOnUnload: true,
        showPopoutIcon: false,
        showMaximiseIcon: true,
        showCloseIcon: false
    },
    content: [{
        type: 'row',
        content:[
            {
                type: 'component',
                componentName: 'view',
                componentState: { model: '{{ embed(roots.controller) }}',
                                  title: 'Controls',
                                  width: 350,
                                  css_classes:['scrollable']},
                isClosable: false,
            },
            {
                type: 'column',
                content: [
                    {
                        type: 'row',
                        content:[
                            {
                                type: 'component',
                                componentName: 'view',
                                componentState: { model: '{{ embed(roots.top_left) }}', title: 'Top Left'},
                                isClosable: false,
                            },
                            {
                                type: 'component',
                                componentName: 'view',
                                componentState: { model: '{{ embed(roots.top_right) }}', title: 'Top Right'},
                                isClosable: false,
                            }
                        ]
                    },
                    {
                        type: 'row',
                        content:[
                            {
                                type: 'component',
                                componentName: 'view',
                                componentState: { model: '{{ embed(roots.bottom_left) }}', title: 'Bottom Left'},
                                isClosable: false,
                            },
                            {
                                type: 'component',
                                componentName: 'view',
                                componentState: { model: '{{ embed(roots.bottom_right) }}', title: 'Bottom Right'},
                                isClosable: false,
                            }
                        ]
                    }
                ]
            }
        ]
    }]
};

{%% if context == 'notebook' %%}
    var myLayout = new GoldenLayout( config, '#' + '{{slicer_id}}' );
    $('#' + '{{slicer_id}}').css({width: '100%%', height: '800px', margin: '0px'})
{%% else %%}
    var myLayout = new GoldenLayout( config );
{%% endif %%}

myLayout.registerComponent('view', function( container, componentState ){
    const {width, css_classes} = componentState
    if(width)
      container.on('open', () => container.setSize(width, container.height))
    if (css_classes)
      css_classes.map((item) => container.getElement().addClass(item))
    container.setTitle(componentState.title)
    container.getElement().html(componentState.model);
    container.on('resize', () => window.dispatchEvent(new Event('resize')))
});

myLayout.init();
</script>
{%% endblock %%}
"""


tmpl = pn.Template(template=(template % 'server'), nb_template=(template % 'notebook'))
tmpl.nb_template.globals['get_id'] = make_globally_unique_id

tmpl.add_panel('controller', controller)
tmpl.add_panel('top_left', top_left)
tmpl.add_panel('top_right', top_right)
tmpl.add_panel('bottom_left', bottom_left)
tmpl.add_panel('bottom_right', bottom_right)

tmpl.servable(title='HoloViz Prototypes')

tmpl.servable(title="HoloViz Prototypes")
