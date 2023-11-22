import { useState } from "react";
// import reactLogo from './assets/react.svg'
// import viteLogo from '/vite.svg'
// import './App.css'

function Movie({ id, description, rating }) {
  return (
    <div className="accordion-item">
      <h2 className="accordion-header">
        <button className="accordion-button" type="button" data-bs-toggle="collapse" data-bs-target={`#collapse${id}`} aria-expanded="true" aria-controls={`collapse${id}`}>
          Movie #{id}
        </button>
      </h2>
      <div id={`collapse${id}`} className="accordion-collapse collapse show" data-bs-parent="#accordionExample">
        <div className="accordion-body">
          <p>{description}</p>
          <p>Avg Rating: {Math.round(rating)}</p>
        </div>
      </div>
    </div>
  );
}

function App() {
  const [query, setQuery] = useState("Give me movies that talk about drugs");
  const [results, setResults] = useState([]);

  const n_results = 5;

  const handleSubmit = (e) => {
    e.preventDefault();
    fetch("http://localhost:5000/api/query", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ query }),
    }).then((res) => {
      return res.json();
    }).then((data) => {

      // console.log(data);
      const result_array = [];

      for (let i = 0; i < n_results; i++) {
        result_array.push({
          'id': data['ids'][0][i],
          'description': data["documents"][0][i],
          'rating': data['metadatas'][0][i]['rating']
        });
      }

      result_array.sort((a, b) => (a.rating > b.rating) ? -1 : 1);

      setResults(result_array);
      console.log(results)

    });
    console.log(query);
  }

  return (
    <>
      <h1>POST /api/query</h1>
      <h2>
        {"{"}<br /><span style={{ marginLeft: "5%" }}>query</span>: {query}<br />{"}"}
      </h2>
      <form onSubmit={handleSubmit}>
        <div className="mb-3">
          <label className="form-label">
            Insert query
          </label>
          <input
            type="text"
            className="form-control"
            id="queryId"
            aria-describedby="queryHelp"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
          />
        </div>
        <button type="submit" className="btn btn-primary">
          Search
        </button>
      </form>

      <div className="accordion" id="accordionExample">
        {results.map((result) => (
          <Movie id={result.id} description={result.description} rating={result.rating} key={result.id} />
        ))}
      </div>
    </>
  );
}

export default App;
