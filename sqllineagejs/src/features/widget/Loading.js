import React from 'react';
import Fade from '@material-ui/core/Fade';
import CircularProgress from '@material-ui/core/CircularProgress';
import {Box} from "@material-ui/core";


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
          transitionDelay: '500ms',
        }}
        unmountOnExit
      >
        <CircularProgress/>
      </Fade>
    </Box>
  );
}
