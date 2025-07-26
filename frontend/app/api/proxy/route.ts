// This file is no longer needed since we're using direct API calls to the backend now.
// CORS issues are fixed directly on the backend API.
// We're keeping this file just for reference.

import { NextRequest, NextResponse } from 'next/server';

// Return a simple message if someone tries to access this route
// eslint-disable-next-line @typescript-eslint/no-unused-vars
export async function GET(_: NextRequest) {
  return new NextResponse(
    JSON.stringify({ message: "This proxy is disabled. Please use direct API calls to the backend." }),
    {
      status: 200,
      headers: {
        'Content-Type': 'application/json',
      },
    }
  );
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
export async function POST(_: NextRequest) {
  return new NextResponse(
    JSON.stringify({ message: "This proxy is disabled. Please use direct API calls to the backend." }),
    {
      status: 200,
      headers: {
        'Content-Type': 'application/json',
      },
    }
  );
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
export async function PUT(_: NextRequest) {
  return new NextResponse(
    JSON.stringify({ message: "This proxy is disabled. Please use direct API calls to the backend." }),
    {
      status: 200,
      headers: {
        'Content-Type': 'application/json',
      },
    }
  );
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
export async function DELETE(_: NextRequest) {
  return new NextResponse(
    JSON.stringify({ message: "This proxy is disabled. Please use direct API calls to the backend." }),
    {
      status: 200,
      headers: {
        'Content-Type': 'application/json',
      },
    }
  );
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
export async function OPTIONS(_: NextRequest) {
  return new NextResponse(
    JSON.stringify({ message: "This proxy is disabled. Please use direct API calls to the backend." }),
    {
      status: 200,
      headers: {
        'Content-Type': 'application/json',
      },
    }
  );
} 