import React from 'react';
import {Box} from "@material-ui/core";
import ErrorOutlinedIcon from "@material-ui/icons/Error";


export function LoadError(props) {
  return (
    <Box
      display="flex"
      justifyContent="center"
      alignItems="center"
      minHeight={props.minHeight}
    >
    <span>
      <ErrorOutlinedIcon color="primary" fontSize="large"/>
      <p>{props.message}</p>
    </span>
    </Box>
  );
}
