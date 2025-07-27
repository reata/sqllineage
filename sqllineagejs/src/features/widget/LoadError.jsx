import React from "react";
import { Box } from "@mui/material";
import EmojiPeopleOutlinedIcon from "@mui/icons-material/EmojiPeopleOutlined";
import ErrorOutlinedIcon from "@mui/icons-material/Error";

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
        <Icon color="primary" fontSize="large" />
        {props.message
          ? props.message
              .split("\n")
              .map((line, idx) => <p key={idx}>{line}</p>)
          : ""}
      </span>
    </Box>
  );
}
