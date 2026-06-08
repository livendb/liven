import { tags as t } from "@lezer/highlight";
import { createTheme } from "@uiw/codemirror-themes";
import { atomoneInit } from "@uiw/codemirror-theme-atomone";

export const atomOneDark = atomoneInit({
  settings: {
    lineHighlight: "transparent",
  },
});

export const gruvboxDark = createTheme({
  theme: "dark",
  settings: {
    background: "#282828",
    foreground: "#ebdbb2",
    caret: "#fdf4c1",
    selection: "#504945",
    selectionMatch: "#504945",
    gutterBackground: "#282828",
    gutterForeground: "#7c6f64",
    gutterBorder: "transparent",
    lineHighlight: "#3c3836",
  },
  styles: [
    {
      tag: [
        t.function(t.variableName),
        t.function(t.propertyName),
        t.url,
        t.processingInstruction,
      ],
      color: "#8ec07c",
    },
    { tag: [t.tagName, t.heading], color: "#fb4934" },
    { tag: t.comment, color: "#928374", fontStyle: "italic" },
    { tag: [t.propertyName], color: "#ebdbb2" },
    { tag: [t.attributeName, t.number], color: "#d3869b" },
    { tag: t.className, color: "#fabd2f" },
    { tag: t.keyword, color: "#fb4934" },
    { tag: [t.string, t.regexp, t.special(t.propertyName)], color: "#b8bb26" },
    { tag: t.operator, color: "#fe8019" },
  ],
});

export const atomOneLight = createTheme({
  theme: "light",
  settings: {
    background: "#fafafa",
    foreground: "#383a42",
    caret: "#526fff",
    selection: "#e5e5e6",
    selectionMatch: "#e5e5e6",
    gutterBackground: "#fafafa",
    gutterForeground: "#a0a1a7",
    gutterBorder: "transparent",
    lineHighlight: "transparent",
  },
  styles: [
    {
      tag: [
        t.function(t.variableName),
        t.function(t.propertyName),
        t.url,
        t.processingInstruction,
      ],
      color: "#4078f2",
    },
    { tag: [t.tagName, t.heading], color: "#e45649" },
    { tag: t.comment, color: "#a0a1a7", fontStyle: "italic" },
    { tag: [t.propertyName], color: "#383a42" },
    { tag: [t.attributeName, t.number], color: "#986801" },
    { tag: t.className, color: "#c18401" },
    { tag: t.keyword, color: "#a626a4" },
    { tag: [t.string, t.regexp, t.special(t.propertyName)], color: "#50a14f" },
  ],
});
