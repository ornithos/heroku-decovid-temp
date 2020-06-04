import dash_html_components as html

table_fontsize = 12
tbl_style = {'font-size': '{:d}px'.format(table_fontsize),
             'padding-top': '0px', 'padding-bottom': '0px'}


def generate_row(dataframe, i, is_title=False):
    if not is_title:
        return [html.Td(dataframe.iloc[i, c], style=tbl_style)
                for c in range(dataframe.shape[1])]
    else:
        title_style = {'font-weight': 'bold', 'font-size': '{:d}px'.format(table_fontsize+1)}
        return [html.Td(html.Span(dataframe.iloc[i, c], style=title_style), style=tbl_style)
                for c in range(dataframe.shape[1])]


def generate_table(dataframe, title_rows, max_rows=30, style={}):
    tbl_style.update(style)

    num_rows = min(len(dataframe), max_rows)
    return html.Table([
        html.Thead(
            html.Tr([html.Th(col) for col in dataframe.columns], style=tbl_style)
        ),
        html.Tbody(
            [html.Tr(generate_row(dataframe, i, title_rows[i]), style=tbl_style)
             for i in range(num_rows)], style=tbl_style)
    ])
