import { createRoot } from "react-dom/client";
import React from "react";
import { Provider } from "react-redux";

import "./index.css";
import App from "./App";
import store from "./app/store";

const root = createRoot(document.getElementById("root"));

root.render(
  <React.StrictMode>
    <Provider store={store}>
      <App />
    </Provider>
  </React.StrictMode>,
);
