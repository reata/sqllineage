import React from 'react';
import {Box} from "@material-ui/core";
import EmojiPeopleOutlinedIcon from '@material-ui/icons/EmojiPeopleOutlined';
import ErrorOutlinedIcon from "@material-ui/icons/Error";


export function LoadError(props) {
  let Icon = props.info ? EmojiPeopleOutlinedIcon : ErrorOutlinedIcon;
  return (
    <Box
      display="flex"
      justifyContent="center"
      alignItems="center"
      minHeight={props.minHeight}
    >
    <span>
      <Icon color="primary" fontSize="large"/>
      {props.message.split("\n").map(line => <p>{line}</p>)}
    </span>
    </Box>
  );
}
