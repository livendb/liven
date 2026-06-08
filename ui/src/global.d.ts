declare module "highlight.js";

declare module "*.css?inline" {
  const content: string;
  export default content;
}
