/** @type {import('next').NextConfig} */
const nextConfig = {
  // Keep any other existing settings
  reactStrictMode: true,
  // Ignore ESLint errors during build
  eslint: {
    ignoreDuringBuilds: true,
  },
  // Ignore TypeScript errors during build
  typescript: {
    ignoreBuildErrors: true,
  },
};

module.exports = nextConfig; 