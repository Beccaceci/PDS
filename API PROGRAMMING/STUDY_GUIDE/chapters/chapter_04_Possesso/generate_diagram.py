import graphviz

dot = graphviz.Digraph(format='png')
dot.attr(rankdir='LR', bgcolor='white', fontname='Helvetica', splines='ortho')
dot.attr('node', fontname='Helvetica', style='filled', color='#ECECEC', fillcolor='#FAFAFA', shape='box', rounded='true')
dot.attr('edge', fontname='Helvetica', color='#333333', arrowsize='0.8')

with dot.subgraph(name='cluster_stack') as c:
    c.attr(label='Stack Memory', style='dashed', color='#AAAAAA', bgcolor='#FDFDFD')
    c.node('s1', 's1\n(ptr, len=5, cap=5)\n[Invalidated]', fontcolor='#888888', color='#CCCCCC', fillcolor='#F0F0F0')
    c.node('s2', 's2\n(ptr, len=5, cap=5)', fontcolor='black', border='2', color='#4A90E2', fillcolor='#E6F2FF')

with dot.subgraph(name='cluster_heap') as c:
    c.attr(label='Heap Memory', style='dashed', color='#AAAAAA', bgcolor='#FDFDFD')
    c.node('data', '"hello"', shape='cylinder', color='#F5A623', fillcolor='#FFF4E5')

dot.edge('s1', 'data', label=' Previously owned', style='dashed', color='#888888', fontcolor='#888888')
dot.edge('s2', 'data', label=' Now owns', color='#4A90E2', fontcolor='#4A90E2', penwidth='2')
dot.edge('s1', 's2', label=' Move semantics\n(let s2 = s1)', style='dotted', color='#E02020', fontcolor='#E02020')

dot.render('/Users/nicolabeccaceci/Desktop/PROGRAMMAZIONE DI SISTEMA/API PROGRAMMING/STUDY_GUIDE/chapters/chapter_04_Possesso/images/ownership_transfer', cleanup=True)
