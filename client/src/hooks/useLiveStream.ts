"use client";

import { useState, useEffect, useRef, useCallback } from "react";

interface UseLiveStreamOptions {
  maxEvents?: number;
  maxReconnectAttempts?: number;
  baseReconnectDelayMs?: number;
  maxReconnectDelayMs?: number;
}

export function useLiveStream<T = any>(
  url: string,
  options: UseLiveStreamOptions = {}
) {
  const {
    maxEvents = 100,
    maxReconnectAttempts = 10,
    baseReconnectDelayMs = 1000,
    maxReconnectDelayMs = 30000,
  } = options;

  const [events, setEvents] = useState<T[]>([]);
  const [isConnected, setIsConnected] = useState(false);

  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const reconnectAttemptsRef = useRef(0);
  const isComponentMounted = useRef(true);

  const connect = useCallback(() => {
    if (!isComponentMounted.current) return;
    if (
      wsRef.current?.readyState === WebSocket.OPEN ||
      wsRef.current?.readyState === WebSocket.CONNECTING
    ) {
      return;
    }

    try {
      const ws = new WebSocket(url);
      wsRef.current = ws;

      ws.onopen = () => {
        if (!isComponentMounted.current) {
          ws.close();
          return;
        }
        setIsConnected(true);
        reconnectAttemptsRef.current = 0;
        console.log(`[useLiveStream] Connected to ${url}`);
      };

      ws.onmessage = (event) => {
        if (!isComponentMounted.current) return;
        try {
          const data = JSON.parse(event.data);
          setEvents((prev) => {
            // Unshift new event to the beginning of the array, keep at most maxEvents
            const newEvents = [data, ...prev];
            return newEvents.slice(0, maxEvents);
          });
        } catch (err) {
          console.error("[useLiveStream] Failed to parse message:", err);
        }
      };

      ws.onclose = () => {
        if (!isComponentMounted.current) return;
        setIsConnected(false);
        wsRef.current = null;
        console.log(`[useLiveStream] Disconnected from ${url}`);

        // Exponential backoff
        if (reconnectAttemptsRef.current < maxReconnectAttempts) {
          const delay = Math.min(
            baseReconnectDelayMs * Math.pow(2, reconnectAttemptsRef.current),
            maxReconnectDelayMs
          );

          if (reconnectTimeoutRef.current) {
            clearTimeout(reconnectTimeoutRef.current);
          }

          console.log(`[useLiveStream] Reconnecting in ${delay}ms...`);
          reconnectTimeoutRef.current = setTimeout(() => {
            reconnectAttemptsRef.current += 1;
            connect();
          }, delay);
        }
      };

      ws.onerror = (error) => {
        // Note: onerror is followed by onclose, which will handle the reconnect logic
        console.error("[useLiveStream] WebSocket error:", error);
      };
    } catch (error) {
      console.error(
        "[useLiveStream] Failed to establish WebSocket connection:",
        error
      );
    }
  }, [
    url,
    maxEvents,
    maxReconnectAttempts,
    baseReconnectDelayMs,
    maxReconnectDelayMs,
  ]);

  useEffect(() => {
    isComponentMounted.current = true;
    connect();

    return () => {
      isComponentMounted.current = false;
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
      }
      if (wsRef.current) {
        wsRef.current.close();
      }
    };
  }, [connect]);

  const clearEvents = useCallback(() => setEvents([]), []);

  return { events, isConnected, clearEvents };
}
