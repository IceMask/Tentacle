package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"mcp_for_appium/internal/device"
	pb "mcp_for_appium/proto/v1"
)

// InteractionService 交互服务实现
type InteractionService struct {
	pb.UnimplementedInteractionServiceServer

	deviceManager  *device.Manager
	sessionService *SessionService
}

// NewInteractionService 创建交互服务
func NewInteractionService(dm *device.Manager) *InteractionService {
	return &InteractionService{
		deviceManager: dm,
	}
}

// SetSessionService 设置会话服务（避免循环依赖）
func (s *InteractionService) SetSessionService(ss *SessionService) {
	s.sessionService = ss
}

// FindElement 查找元素
func (s *InteractionService) FindElement(ctx context.Context, req *pb.FindElementRequest) (*pb.FindElementResponse, error) {
	// 获取会话信息
	sessionInfo, err := s.sessionService.GetSessionInfo(req.SessionId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "session not found: %v", err)
	}

	// 构建查找元素的请求
	strategy := locatorStrategyToString(req.Locator.Strategy)

	body := map[string]interface{}{
		"using": strategy,
		"value": req.Locator.Value,
	}

	// 如果指定了上下文元素，在该元素内查找
	path := fmt.Sprintf("/session/%s/element", sessionInfo.AppiumClient.GetSessionID())
	if req.ContextElementId != "" {
		path = fmt.Sprintf("/session/%s/element/%s/element",
			sessionInfo.AppiumClient.GetSessionID(), req.ContextElementId)
	}

	// 实现超时重试
	timeout := req.Timeout.AsDuration()
	if timeout == 0 {
		timeout = 10 * time.Second
	}

	deadline := time.Now().Add(timeout)
	var lastErr error

	for time.Now().Before(deadline) {
		respData, err := sessionInfo.AppiumClient.Request(ctx, "POST", path, body)
		if err == nil {
			// 解析响应
			var elementResp struct {
				Element string `json:"element-6066-11e4-a52e-4f735466cecf"`
			}

			if err := json.Unmarshal(respData, &elementResp); err != nil {
				return nil, status.Errorf(codes.Internal, "failed to parse element response: %v", err)
			}

			// 获取元素详细信息
			element, err := s.getElementInfo(ctx, sessionInfo, elementResp.Element)
			if err != nil {
				return nil, err
			}

			return &pb.FindElementResponse{
				Element: element,
				Found:   true,
			}, nil
		}

		lastErr = err

		// 如果需要滚动查找
		if req.ScrollIntoView {
			// 尝试向下滚动
			_ = s.performSwipe(ctx, sessionInfo, 0.5, 0.8, 0.5, 0.2)
		}

		time.Sleep(500 * time.Millisecond)
	}

	return &pb.FindElementResponse{
		Found:        false,
		ErrorMessage: fmt.Sprintf("element not found: %v", lastErr),
	}, nil
}

// FindElements 查找多个元素
func (s *InteractionService) FindElements(ctx context.Context, req *pb.FindElementsRequest) (*pb.FindElementsResponse, error) {
	sessionInfo, err := s.sessionService.GetSessionInfo(req.SessionId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "session not found: %v", err)
	}

	strategy := locatorStrategyToString(req.Locator.Strategy)

	body := map[string]interface{}{
		"using": strategy,
		"value": req.Locator.Value,
	}

	path := fmt.Sprintf("/session/%s/elements", sessionInfo.AppiumClient.GetSessionID())
	if req.ContextElementId != "" {
		path = fmt.Sprintf("/session/%s/element/%s/elements",
			sessionInfo.AppiumClient.GetSessionID(), req.ContextElementId)
	}

	respData, err := sessionInfo.AppiumClient.Request(ctx, "POST", path, body)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to find elements: %v", err)
	}

	// 解析响应
	var elementsResp []struct {
		Element string `json:"element-6066-11e4-a52e-4f735466cecf"`
	}

	if err := json.Unmarshal(respData, &elementsResp); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to parse elements response: %v", err)
	}

	// 获取每个元素的详细信息
	elements := make([]*pb.Element, 0, len(elementsResp))
	for i, elemResp := range elementsResp {
		// 限制结果数量
		if req.MaxResults > 0 && int32(i) >= req.MaxResults {
			break
		}

		element, err := s.getElementInfo(ctx, sessionInfo, elemResp.Element)
		if err != nil {
			continue // 跳过获取失败的元素
		}
		elements = append(elements, element)
	}

	return &pb.FindElementsResponse{
		Elements:   elements,
		TotalFound: int32(len(elementsResp)),
	}, nil
}

// Tap 点击
func (s *InteractionService) Tap(ctx context.Context, req *pb.TapRequest) (*pb.TapResponse, error) {
	sessionInfo, err := s.sessionService.GetSessionInfo(req.SessionId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "session not found: %v", err)
	}

	var tapErr error

	switch target := req.Target.(type) {
	case *pb.TapRequest_ElementId:
		// 点击元素
		path := fmt.Sprintf("/session/%s/element/%s/click",
			sessionInfo.AppiumClient.GetSessionID(), target.ElementId)
		_, tapErr = sessionInfo.AppiumClient.Request(ctx, "POST", path, map[string]interface{}{})

	case *pb.TapRequest_Coordinates:
		// 点击坐标
		actions := map[string]interface{}{
			"actions": []interface{}{
				map[string]interface{}{
					"type": "pointer",
					"id":   "finger1",
					"parameters": map[string]interface{}{
						"pointerType": "touch",
					},
					"actions": []interface{}{
						map[string]interface{}{
							"type":     "pointerMove",
							"duration": 0,
							"x":        target.Coordinates.X,
							"y":        target.Coordinates.Y,
						},
						map[string]interface{}{
							"type":   "pointerDown",
							"button": 0,
						},
						map[string]interface{}{
							"type":   "pointerUp",
							"button": 0,
						},
					},
				},
			},
		}

		path := fmt.Sprintf("/session/%s/actions", sessionInfo.AppiumClient.GetSessionID())
		_, tapErr = sessionInfo.AppiumClient.Request(ctx, "POST", path, actions)
	}

	if tapErr != nil {
		return &pb.TapResponse{
			Success:      false,
			ErrorMessage: tapErr.Error(),
			ExecutedAt:   timestamppb.Now(),
		}, nil
	}

	return &pb.TapResponse{
		Success:    true,
		ExecutedAt: timestamppb.Now(),
	}, nil
}

// InputText 输入文本
func (s *InteractionService) InputText(ctx context.Context, req *pb.InputTextRequest) (*pb.InputTextResponse, error) {
	sessionInfo, err := s.sessionService.GetSessionInfo(req.SessionId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "session not found: %v", err)
	}

	// 清空文本（如果需要）
	if req.ClearFirst {
		clearPath := fmt.Sprintf("/session/%s/element/%s/clear",
			sessionInfo.AppiumClient.GetSessionID(), req.ElementId)
		_, _ = sessionInfo.AppiumClient.Request(ctx, "POST", clearPath, map[string]interface{}{})
	}

	// 输入文本
	body := map[string]interface{}{
		"text":  req.Text,
		"value": []string{req.Text}, // 某些版本的 Appium 需要这个格式
	}

	path := fmt.Sprintf("/session/%s/element/%s/value",
		sessionInfo.AppiumClient.GetSessionID(), req.ElementId)

	_, err = sessionInfo.AppiumClient.Request(ctx, "POST", path, body)
	if err != nil {
		return &pb.InputTextResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	// 隐藏键盘（如果需要）
	if req.HideKeyboard {
		hidePath := fmt.Sprintf("/session/%s/appium/device/hide_keyboard",
			sessionInfo.AppiumClient.GetSessionID())
		_, _ = sessionInfo.AppiumClient.Request(ctx, "POST", hidePath, map[string]interface{}{})
	}

	// 获取实际文本
	actualText := ""
	textPath := fmt.Sprintf("/session/%s/element/%s/text",
		sessionInfo.AppiumClient.GetSessionID(), req.ElementId)
	if respData, err := sessionInfo.AppiumClient.Request(ctx, "GET", textPath, nil); err == nil {
		json.Unmarshal(respData, &actualText)
	}

	return &pb.InputTextResponse{
		Success:    true,
		ActualText: actualText,
	}, nil
}

// Swipe 滑动
func (s *InteractionService) Swipe(ctx context.Context, req *pb.SwipeRequest) (*pb.SwipeResponse, error) {
	sessionInfo, err := s.sessionService.GetSessionInfo(req.SessionId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "session not found: %v", err)
	}

	duration := req.Duration.AsDuration()
	if duration == 0 {
		duration = 1 * time.Second
	}

	// 使用 W3C Actions API
	actions := map[string]interface{}{
		"actions": []interface{}{
			map[string]interface{}{
				"type": "pointer",
				"id":   "finger1",
				"parameters": map[string]interface{}{
					"pointerType": "touch",
				},
				"actions": []interface{}{
					map[string]interface{}{
						"type":     "pointerMove",
						"duration": 0,
						"x":        req.Start.X,
						"y":        req.Start.Y,
					},
					map[string]interface{}{
						"type":   "pointerDown",
						"button": 0,
					},
					map[string]interface{}{
						"type":     "pointerMove",
						"duration": int(duration.Milliseconds()),
						"x":        req.End.X,
						"y":        req.End.Y,
					},
					map[string]interface{}{
						"type":   "pointerUp",
						"button": 0,
					},
				},
			},
		},
	}

	path := fmt.Sprintf("/session/%s/actions", sessionInfo.AppiumClient.GetSessionID())
	_, err = sessionInfo.AppiumClient.Request(ctx, "POST", path, actions)

	if err != nil {
		return &pb.SwipeResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	return &pb.SwipeResponse{
		Success:        true,
		ActualDuration: durationpb.New(duration),
	}, nil
}

// LongPress 长按
func (s *InteractionService) LongPress(ctx context.Context, req *pb.LongPressRequest) (*pb.LongPressResponse, error) {
	sessionInfo, err := s.sessionService.GetSessionInfo(req.SessionId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "session not found: %v", err)
	}

	duration := req.Duration.AsDuration()
	if duration == 0 {
		duration = 1 * time.Second
	}

	var x, y int32

	// 获取坐标
	switch target := req.Target.(type) {
	case *pb.LongPressRequest_ElementId:
		// 获取元素中心点坐标
		locationPath := fmt.Sprintf("/session/%s/element/%s/location",
			sessionInfo.AppiumClient.GetSessionID(), target.ElementId)
		sizePath := fmt.Sprintf("/session/%s/element/%s/size",
			sessionInfo.AppiumClient.GetSessionID(), target.ElementId)

		var location struct{ X, Y int32 }
		var size struct{ Width, Height int32 }

		if respData, err := sessionInfo.AppiumClient.Request(ctx, "GET", locationPath, nil); err == nil {
			json.Unmarshal(respData, &location)
		}
		if respData, err := sessionInfo.AppiumClient.Request(ctx, "GET", sizePath, nil); err == nil {
			json.Unmarshal(respData, &size)
		}

		x = location.X + size.Width/2
		y = location.Y + size.Height/2

	case *pb.LongPressRequest_Coordinates:
		x = target.Coordinates.X
		y = target.Coordinates.Y
	}

	// 执行长按
	actions := map[string]interface{}{
		"actions": []interface{}{
			map[string]interface{}{
				"type": "pointer",
				"id":   "finger1",
				"parameters": map[string]interface{}{
					"pointerType": "touch",
				},
				"actions": []interface{}{
					map[string]interface{}{
						"type":     "pointerMove",
						"duration": 0,
						"x":        x,
						"y":        y,
					},
					map[string]interface{}{
						"type":   "pointerDown",
						"button": 0,
					},
					map[string]interface{}{
						"type":     "pause",
						"duration": int(duration.Milliseconds()),
					},
					map[string]interface{}{
						"type":   "pointerUp",
						"button": 0,
					},
				},
			},
		},
	}

	path := fmt.Sprintf("/session/%s/actions", sessionInfo.AppiumClient.GetSessionID())
	_, err = sessionInfo.AppiumClient.Request(ctx, "POST", path, actions)

	if err != nil {
		return &pb.LongPressResponse{
			Success:      false,
			ErrorMessage: err.Error(),
			ExecutedAt:   timestamppb.Now(),
		}, nil
	}

	return &pb.LongPressResponse{
		Success:    true,
		ExecutedAt: timestamppb.Now(),
	}, nil
}

// 辅助方法

// getElementInfo 获取元素详细信息
func (s *InteractionService) getElementInfo(ctx context.Context, sessionInfo *SessionInfo, elementID string) (*pb.Element, error) {
	element := &pb.Element{
		Id:         elementID,
		Attributes: make(map[string]string),
	}

	// 获取元素属性
	sessionID := sessionInfo.AppiumClient.GetSessionID()

	// 获取位置和大小
	if respData, err := sessionInfo.AppiumClient.Request(ctx, "GET",
		fmt.Sprintf("/session/%s/element/%s/rect", sessionID, elementID), nil); err == nil {
		var rect struct {
			X      int32 `json:"x"`
			Y      int32 `json:"y"`
			Width  int32 `json:"width"`
			Height int32 `json:"height"`
		}
		if json.Unmarshal(respData, &rect) == nil {
			element.Bounds = &pb.Rectangle{
				X:      rect.X,
				Y:      rect.Y,
				Width:  rect.Width,
				Height: rect.Height,
			}
		}
	}

	// 获取常用属性
	attributes := map[string]string{
		"text":  "text",
		"name":  "name",
		"value": "value",
		"type":  "type",
		"label": "label",
	}

	for attrName, attrPath := range attributes {
		if respData, err := sessionInfo.AppiumClient.Request(ctx, "GET",
			fmt.Sprintf("/session/%s/element/%s/attribute/%s", sessionID, elementID, attrPath), nil); err == nil {
			var value string
			if json.Unmarshal(respData, &value) == nil && value != "" {
				element.Attributes[attrName] = value
			}
		}
	}

	// 获取状态
	if respData, err := sessionInfo.AppiumClient.Request(ctx, "GET",
		fmt.Sprintf("/session/%s/element/%s/displayed", sessionID, elementID), nil); err == nil {
		json.Unmarshal(respData, &element.Visible)
	}

	if respData, err := sessionInfo.AppiumClient.Request(ctx, "GET",
		fmt.Sprintf("/session/%s/element/%s/enabled", sessionID, elementID), nil); err == nil {
		json.Unmarshal(respData, &element.Enabled)
	}

	if respData, err := sessionInfo.AppiumClient.Request(ctx, "GET",
		fmt.Sprintf("/session/%s/element/%s/selected", sessionID, elementID), nil); err == nil {
		json.Unmarshal(respData, &element.Selected)
	}

	return element, nil
}

// performSwipe 执行滑动（内部使用）
func (s *InteractionService) performSwipe(ctx context.Context, sessionInfo *SessionInfo,
	startXRatio, startYRatio, endXRatio, endYRatio float64) error {

	// 获取屏幕尺寸
	windowSize := struct {
		Width  int `json:"width"`
		Height int `json:"height"`
	}{}

	if respData, err := sessionInfo.AppiumClient.Request(ctx, "GET",
		fmt.Sprintf("/session/%s/window/rect", sessionInfo.AppiumClient.GetSessionID()), nil); err == nil {
		json.Unmarshal(respData, &windowSize)
	} else {
		// 默认尺寸
		windowSize.Width = 1080
		windowSize.Height = 1920
	}

	swipeReq := &pb.SwipeRequest{
		SessionId: sessionInfo.Session.Id,
		Start: &pb.Point{
			X: int32(float64(windowSize.Width) * startXRatio),
			Y: int32(float64(windowSize.Height) * startYRatio),
		},
		End: &pb.Point{
			X: int32(float64(windowSize.Width) * endXRatio),
			Y: int32(float64(windowSize.Height) * endYRatio),
		},
		Duration: durationpb.New(500 * time.Millisecond),
	}

	_, err := s.Swipe(ctx, swipeReq)
	return err
}

// locatorStrategyToString 转换定位策略
func locatorStrategyToString(strategy pb.LocatorStrategy) string {
	switch strategy {
	case pb.LocatorStrategy_LOCATOR_STRATEGY_ID:
		return "id"
	case pb.LocatorStrategy_LOCATOR_STRATEGY_CLASS:
		return "class name"
	case pb.LocatorStrategy_LOCATOR_STRATEGY_XPATH:
		return "xpath"
	case pb.LocatorStrategy_LOCATOR_STRATEGY_NAME:
		return "name"
	case pb.LocatorStrategy_LOCATOR_STRATEGY_ACCESSIBILITY_ID:
		return "accessibility id"
	case pb.LocatorStrategy_LOCATOR_STRATEGY_CSS:
		return "css selector"
	case pb.LocatorStrategy_LOCATOR_STRATEGY_LINK_TEXT:
		return "link text"
	case pb.LocatorStrategy_LOCATOR_STRATEGY_PARTIAL_LINK_TEXT:
		return "partial link text"
	case pb.LocatorStrategy_LOCATOR_STRATEGY_PREDICATE:
		return "-ios predicate string"
	case pb.LocatorStrategy_LOCATOR_STRATEGY_CLASS_CHAIN:
		return "-ios class chain"
	case pb.LocatorStrategy_LOCATOR_STRATEGY_IMAGE:
		return "-image"
	default:
		return "xpath"
	}
}
