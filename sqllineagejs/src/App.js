import React from 'react';
import CytoscapeComponent from 'react-cytoscapejs';
import dagre from 'cytoscape-dagre';
import cytoscape from 'cytoscape';

cytoscape.use(dagre);

class App extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      error: null,
      isLoaded: false,
      elements: []
    }
  }

  componentDidMount() {
    const url = new URL(window.location.href);
    const backend_port = process.env.REACT_APP_BACKEND_PORT;
    const backend_origin = backend_port ? url.origin.replace(url.port, backend_port) : url.origin;
    fetch(`${backend_origin}/lineage`, {
      method: 'POST',
      body: JSON.stringify(Object.fromEntries(url.searchParams)),
      headers: new Headers({
        'Content-Type': 'application/json'
      })
    })
      .then(res => res.json())
      .then(
        (result) => {
          this.setState({
            isLoaded: true,
            elements: result
          })
        },
        (error) => {
          this.setState({
            isLoaded: true,
            error
          })
        }
      )
  }

  render() {
    const {error, isLoaded, elements} = this.state;
    if (error) {
      return <div>Error: {error.message}</div>;
    } else if (!isLoaded) {
      return <div>Loading...</div>;
    } else {
      const stylesheet = [
        {
          selector: 'node',
          style: {
            height: 10,
            width: 10,
            content: 'data(id)',
            'text-valign': 'top',
            'text-halign': 'right',
            'font-size': 10,
            'color': '#35393e',
            'background-color': '#3499d9',
            'border-color': '#000',
            'border-width': 1,
            'border-opacity': 0.8
          }
        },
        {
          selector: 'edge',
          style: {
            width: 1,
            'line-color': '#9ab5c7',
            'target-arrow-color': '#9ab5c7',
            'target-arrow-shape': 'triangle',
            'arrow-scale': 0.8,
            'curve-style': 'bezier'
          }
        }
      ]
      const layout = {
        name: 'dagre',
        rankDir: 'LR',
        rankSep: 200,
      };
      const style = {width: '1920px', height: '1080px'};
      return <CytoscapeComponent
        elements={elements}
        stylesheet={stylesheet}
        style={style}
        layout={layout}
        zoom={1}
        minZoom={0.5}
        maxZoom={2}
      />;
    }
  }
}

export default App;
