import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { createBrowserRouter, RouterProvider, useNavigate, useParams } from "react-router-dom";
import { Shell } from "./components/Shell";
import { DeadLetters } from "./pages/DeadLetters";
import { Jobs } from "./pages/Jobs";
import { Overview } from "./pages/Overview";
import { Schedules } from "./pages/Schedules";
import { Tenants } from "./pages/Tenants";
import { WorkflowDetail, Workflows } from "./pages/Workflows";
import "./styles.css";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: { retry: 1, refetchOnWindowFocus: true, placeholderData: (previous: unknown) => previous },
  },
});

function NotFound() {
  const navigate = useNavigate();
  const params = useParams();
  return (
    <div className="empty">
      <h3>There's nothing at this address</h3>
      <p>{params["*"] ? `“/${params["*"]}” isn't a page in the console.` : "This page doesn't exist."}</p>
      <button className="btn" onClick={() => navigate("/")}>
        Go to the board
      </button>
    </div>
  );
}

const router = createBrowserRouter([
  {
    path: "/",
    element: <Shell />,
    children: [
      { index: true, element: <Overview /> },
      { path: "jobs", element: <Jobs /> },
      { path: "jobs/:id", element: <Jobs /> },
      { path: "workflows", element: <Workflows /> },
      { path: "workflows/:id", element: <WorkflowDetail /> },
      { path: "dead", element: <DeadLetters /> },
      { path: "schedules", element: <Schedules /> },
      { path: "tenants", element: <Tenants /> },
      { path: "*", element: <NotFound /> },
    ],
  },
]);

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>
  </StrictMode>,
);
