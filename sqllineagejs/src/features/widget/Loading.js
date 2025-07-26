import React from "react";
import Fade from "@mui/material/Fade";
import CircularProgress from "@mui/material/CircularProgress";
import { Box } from "@mui/material";

export function Loading(props) {
  return (
    <Box
      display="flex"
      justifyContent="center"
      alignItems="center"
      minHeight={props.minHeight}
    >
      <Fade
        in
        style={{
          transitionDelay: "500ms",
        }}
        unmountOnExit
      >
        <CircularProgress />
      </Fade>
    </Box>
  );
}
