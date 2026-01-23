import { defineConfig } from "vite"
import react from "@vitejs/plugin-react"

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/cbmt-style': {
        target: 'https://arcgis.com',
        changeOrigin: true,
        secure: true,
        rewrite: (path) =>
          path.replace(
            /^\/cbmt-style/,
            '/sharing/rest/content/items/708e92c1f00941e3af3dd3c092ae4a0a/resources/styles/root.json'
          ),
      },
    },
  },
});
